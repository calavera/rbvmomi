class RbVmomi::VIM::OvfManager
  require 'uri'
  require 'open-uri'

  # Deploy an OVF.
  #
  # @param [Hash] opts The options hash.
  # @option opts [String]             :uri Location of the OVF.
  # @option opts [String]             :vmName Name of the new VM.
  # @option opts [VIM::Folder]        :vmFolder Folder to place the VM in.
  # @option opts [VIM::HostSystem]    :host Host to use.
  # @option opts [VIM::ResourcePool]  :resourcePool Resource pool to use.
  # @option opts [VIM::Datastore]     :datastore Datastore to use.
  # @option opts [String]             :diskProvisioning (thin) Disk provisioning mode.
  # @option opts [Hash]               :networkMappings Network mappings.
  # @option opts [Hash]               :propertyMappings Property mappings.
  def deployOVF opts
    opts = { :networkMappings => {},
             :propertyMappings => {},
             :diskProvisioning => :thin }.merge opts

    %w(uri vmName vmFolder host resourcePool datastore).each do |k|
      fail "parameter #{k} required" unless opts[k.to_sym]
    end

    ovfImportSpec = RbVmomi::VIM::OvfCreateImportSpecParams(
      :hostSystem => opts[:host],
      :locale => "US",
      :entityName => opts[:vmName],
      :deploymentOption => "",
      :networkMapping => opts[:networkMappings].map{|from, to| RbVmomi::VIM::OvfNetworkMapping(:name => from, :network => to)},
      :propertyMapping => opts[:propertyMappings].to_a,
      :diskProvisioning => opts[:diskProvisioning]
    )

    result = CreateImportSpec(
      :ovfDescriptor => open(opts[:uri]).read,
      :resourcePool => opts[:resourcePool],
      :datastore => opts[:datastore],
      :cisp => ovfImportSpec
    )

    raise result.error[0].localizedMessage if result.error && !result.error.empty?

    if result.warning
      result.warning.each{|x| puts "OVF Warning: #{x.localizedMessage.chomp}" }
    end

    nfcLease = opts[:resourcePool].ImportVApp(:spec => result.importSpec,
                                              :folder => opts[:vmFolder],
                                              :host => opts[:host])

    nfcLease.wait_until(:state) { nfcLease.state != "initializing" }
    raise nfcLease.error if nfcLease.state == "error"
    begin
      nfcLease.HttpNfcLeaseProgress(:percent => 5)

      result.fileItem.each do |fileItem|
        deviceUrl = nfcLease.info.deviceUrl.find{|x| x.importKey == fileItem.deviceId}
        if !deviceUrl
          raise "Couldn't find deviceURL for device '#{fileItem.deviceId}'"
        end

        device_href = deviceUrl.url.gsub("*", opts[:host].config.network.vnic[0].spec.ip.ipAddress)
        ovf_uri = opts[:uri].to_s

        stream_disk(ovf_uri, fileItem.path, device_href) do |uploaded, total|
          progress = 5 + ((uploaded * 90) / total)
          nfcLease.HttpNfcLeaseProgress(:percent => progress.to_i)
        end
      end

      nfcLease.HttpNfcLeaseProgress(:percent => 100)
      vm = nfcLease.info.entity
      nfcLease.HttpNfcLeaseComplete
      vm
    end
  rescue Exception
    (nfcLease.HttpNfcLeaseAbort rescue nil) if nfcLease
    raise
  end

  def stream_disk(ovf_uri, disk_path, target_uri, &block)
    uri = URI.parse(ovf_uri)
    target_uri = URI.escape(target_uri)

    pbar = ProgressBar.new "Progress", 100
    if uri.scheme.nil?
      # local path
      file = File.open(File.expand_path(disk_path, File.dirname(ovf_uri)))

      total_bytes = file.size
      uploaded_bytes = 0
      chunker = lambda do
        uploaded_bytes += Excon::CHUNK_SIZE

        pbar.set ((uploaded_bytes * 100) / total_bytes).to_i

        block.call(uploaded_bytes, total_bytes) if block_given?
        file.read(Excon::CHUNK_SIZE).to_s
      end

      Excon.post(target_uri, :request_block => chunker)

      file.close
    else
      # same hack that we had before
      tmp = ovf_uri.split(/\//)
      tmp.pop
      tmp << disk_path
      disk_uri = tmp.join("/")

      Excon.pipe(disk_uri, target_uri) do |chunk, remaining_bytes, total_bytes|
        uploaded_bytes = total_bytes - remaining_bytes

        pbar.set ((uploaded_bytes * 100) / total_bytes).to_i

        block.call(uploaded_bytes, total_bytes) if block_given?
      end
    end

    pbar.finish
  end
end
