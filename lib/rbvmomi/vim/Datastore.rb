class RbVmomi::VIM::Datastore

  # Check whether a file exists on this datastore.
  # @param path [String] Path on the datastore.
  def exists? path
    req = Net::HTTP::Head.new mkuripath(path)
    req.initialize_http_header 'cookie' => _connection.cookie
    resp = _connection.http.request req
    case resp
    when Net::HTTPSuccess
      true
    when Net::HTTPNotFound
      false
    else
      fail resp.inspect
    end
  end

  # Download a file from this datastore.
  # @param remote_path [String] Source path on the datastore.
  # @param local_path [String] Destination path on the local machine.
  # @return [void]
  def download remote_path, local_path
    url = "http#{_connection.http.use_ssl? ? 's' : ''}://#{_connection.http.address}:#{_connection.http.port}#{mkuripath(remote_path)}"
    response = Excon.get(url, :headers => {:Cookie => _connection.cookie}, :expects => [200])
    File.open(local_path, 'w') {|f| f.write response.body}
  end

  # Upload a file to this datastore.
  # @param remote_path [String] Destination path on the datastore.
  # @param local_path [String] Source path on the local machine.
  # @return [void]
  def upload remote_path, local_path
    url = "http#{_connection.http.use_ssl? ? 's' : ''}://#{_connection.http.address}:#{_connection.http.port}#{mkuripath(remote_path)}"

    pbar = ProgressBar.new "Progress", 100
    file = File.open(local_path)

    total_bytes = file.size
    uploaded_bytes = 0
    chunker = lambda do
      uploaded_bytes += Excon::CHUNK_SIZE

      pbar.set ((uploaded_bytes * 100) / total_bytes).to_i

      file.read(Excon::CHUNK_SIZE).to_s
    end

    Excon.post(url, :request_block => chunker, :headers => {:Cookie => _connection.cookie}, :expects => [200, 201])

    pbar.finish
  end

  private

  def datacenter
    return @datacenter if @datacenter
    x = parent
    while not x.is_a? RbVmomi::VIM::Datacenter
      x = x.parent
    end
    fail unless x.is_a? RbVmomi::VIM::Datacenter
    @datacenter = x
  end

  def mkuripath path
    "/folder/#{URI.escape path}?dcPath=#{URI.escape datacenter.name}&dsName=#{URI.escape name}"
  end
end
