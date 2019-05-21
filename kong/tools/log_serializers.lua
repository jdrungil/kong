local Serializers = {}

local chunks = {}

local function load_serializer_from_db(name)
  local row, err = kong.db.log_serializers:select_by_name(name)
  if err then
    return nil, err
  end

  if not row then
    return nil, "serializer '" .. name .. "' not found"
  end

  return row.chunk
end

-- fetch the serializer from the DB via kong.cache
function Serializers.load_serializer(name)
  -- already have it
  if chunks[name] then
    return true
  end

  local chunk, err = load_serializer_from_db(name)
  if err then
    return nil, err
  end

  local s = loadstring(ngx.decode_base64(chunk))
  if not s then
    return nil, "failed to load serializer chunk"
  end

  chunks[name] = s().serialize

  return true
end

-- return the serializer function from our cache
function Serializers.get_serializer(name)
  if not chunks[name] then
    return nil, "serializer '" .. name .. "' not found"
  end

  return chunks[name]
end

function Serializers.clear_serializer(name)
  chunks[name] = nil
end

return Serializers
