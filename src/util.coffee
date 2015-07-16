isRaw = module.exports.isRaw = (x) ->
    x? and ('object' is typeof x) and ('function' is typeof x.sql)

asRaw = module.exports.asRaw = (x) ->
    if isRaw x
        return x

    unless 'string' is typeof x
        throw new Exception 'raw or string expected'

    {
        sql: -> x
        params: -> []
    }

extend = module.exports.extend = (dest, src) ->
    for key, val of src
        dest[key] = val
    dest