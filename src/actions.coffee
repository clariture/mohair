{isRaw, asRaw} = require './util'

insertPrototype =
    sql: (mohair) ->
        that = this

        unless mohair._table?
            throw new Error 'sql of insert requires call to table before it'

        table = mohair._escape mohair._table
        sql = ''

        if mohair._with?
            sql += 'WITH '
            parts = []
            parts = Object.keys(mohair._with).map (key) ->
                key + ' AS (' + asRaw(mohair._with[key]).sql() + ')'
            sql += parts.join(', ')
            sql += ' '

        if isRaw that._data
            keys = mohair._attributes
            unless keys?
                throw new Error 'sql of insert requires call to attributes before it'
            if keys.length is 0
                throw new Error 'sql of insert requires one or more attributes'

            escapedKeys = keys.map (key) -> mohair._escape key
            sql += "INSERT INTO #{table}(#{escapedKeys.join ', '}) "

            sql += that._data.sql()
        else
            keys = mohair._attributes || Object.keys(that._data[0])
            unless keys?
                throw new Error 'sql of insert requires call to attributes before it'
            if keys.length is 0
                throw new Error 'sql of insert requires one or more attributes'

            escapedKeys = keys.map (key) -> mohair._escape key
            sql += "INSERT INTO #{table}(#{escapedKeys.join ', '}) "

            rows = that._data.map (d) ->
                row = keys.map (k) ->
                    if isRaw d[k]
                        d[k].sql()
                    else
                        '?'
                "(#{row.join ', '})"
            sql += "VALUES #{rows.join ', '}"

    params: (mohair) ->
        that = this
        params = []

        if mohair._with?
            Object.keys(mohair._with).forEach (key) ->
                params = params.concat asRaw(mohair._with[key]).params()

        if isRaw that._data
            params = params.concat that._data.params()
        else
            keys = mohair._attributes || Object.keys(that._data[0])
            unless keys?
                throw new Error 'sql of insert requires call to attributes before it'
            if keys.length is 0
                throw new Error 'sql of insert requires one or more attributes'

            that._data.forEach (d) ->
                keys.forEach (k) ->
                    if isRaw d[k]
                        params = params.concat d[k].params()
                    else
                        params.push d[k]

        params

module.exports.insert = (data) ->
    if isRaw data
        processedData = data
    else if data?
        processedData = if Array.isArray(data) then data else [data]
        if processedData.length is 0
            throw new Error 'insert data argument is empty - no records to insert'
        processedData.forEach (d) ->
            unless 'object' is typeof d
                throw new Error 'insert data argument must be an object'
    else
        throw new Error 'insert data argument must be supplied'

    object = Object.create insertPrototype
    object._data = processedData
    object

selectPrototype =
    sql: (mohair) ->
        that = this
        table = mohair._escape mohair._table
        sql = ''

        if mohair._with?
            sql += 'WITH '
            parts = []
            parts = Object.keys(mohair._with).map (key) ->
                key + ' AS (' + asRaw(mohair._with[key]).sql() + ')'
            sql += parts.join(', ')
            sql += ' '

        sql += "SELECT "
        sql += "DISTINCT " if mohair._distinct?
        sql += that._select.sql()

        parts = []
        parts.push "#{table}" if mohair._table?
        parts.push "#{mohair._from.sql()}" if mohair._from?
        sql += " FROM " + parts.join ', ' if parts.length

        mohair._joins.forEach (join) ->
            sql += " #{join.sql()}"
        sql += " WHERE #{mohair._where.sql()}" if mohair._where?
        sql += " GROUP BY #{mohair._group}" if mohair._group?
        sql += " HAVING #{mohair._having.sql()}" if mohair._having?
        sql += " ORDER BY #{mohair._order.sql()}" if mohair._order?
        sql += " LIMIT ?" if mohair._limit?
        sql += " OFFSET ?" if mohair._offset?
        sql
    params: (mohair) ->
        that = this
        params = []

        if mohair._with?
            Object.keys(mohair._with).forEach (key) ->
                params = params.concat asRaw(mohair._with[key]).params()

        params = params.concat that._select.params()

        params = params.concat mohair._from.params() if mohair._from?

        mohair._joins.forEach (join) ->
            params = params.concat join.params()

        params = params.concat mohair._where.params() if mohair._where?
        params = params.concat mohair._having.params() if mohair._having?
        params = params.concat mohair._order.params() if mohair._order?
        params.push mohair._limit if mohair._limit?
        params.push mohair._offset if mohair._offset?
        params

module.exports.select = (select) ->
    object = Object.create selectPrototype
    object._select = select
    object

updatePrototype =
    sql: (mohair) ->
        that = this

        unless mohair._table?
            throw new Error 'sql of update requires call to table before it'

        table = mohair._escape mohair._table
        sql = ''

        if mohair._with?
            sql += 'WITH '
            parts = []
            parts = Object.keys(mohair._with).map (key) ->
                key + ' AS (' + asRaw(mohair._with[key]).sql() + ')'
            sql += parts.join(', ')
            sql += ' '

        keys = mohair._attributes || Object.keys(that._data)
        unless keys?
            throw new Error 'sql of update requires call to attributes before it'
        if keys.length is 0
            throw new Error 'sql of update requires one or more attributes'

        updates = keys.map (key) ->
            escapedKey = mohair._escape key
            if isRaw that._data[key]
                "#{escapedKey} = #{that._data[key].sql()}"
            else
                "#{escapedKey} = ?"

        sql += "UPDATE #{table} SET #{updates.join ', '}"
        sql += " FROM #{mohair._from.sql()}" if mohair._from?
        sql += " WHERE #{mohair._where.sql()}" if mohair._where?
        sql
    params: (mohair) ->
        that = this
        params = []

        if mohair._with?
            Object.keys(mohair._with).forEach (key) ->
                params = params.concat asRaw(mohair._with[key]).params()

        keys = mohair._attributes || Object.keys(that._data)
        unless keys?
            throw new Error 'sql of update requires call to attributes before it'
        if keys.length is 0
            throw new Error 'sql of update requires one or more attributes'

        keys.forEach (k) ->
            if isRaw that._data[k]
                params = params.concat that._data[k].params()
            else
                params.push that._data[k]

        params = params.concat mohair._from.params() if mohair._from?
        params = params.concat mohair._where.params() if mohair._where?

        params

module.exports.update = (data) ->
    if data?
        unless 'object' is typeof data
            throw new Error 'update data argument must be an object'
    else
        throw new Error 'update data argument must be supplied'

    object = Object.create updatePrototype
    object._data = data
    object

deletePrototype =
    sql: (mohair) ->
        that = this

        unless mohair._table?
            throw new Error 'sql of delete requires call to table before it'

        table = mohair._escape mohair._table
        sql = ''

        if mohair._with?
            sql += 'WITH '
            parts = []
            parts = Object.keys(mohair._with).map (key) ->
                key + ' AS (' + asRaw(mohair._with[key]).sql() + ')'
            sql += parts.join(', ')
            sql += ' '

        sql += "DELETE FROM #{table}"
        sql += " USING #{mohair._using.sql()}" if mohair._using?
        sql += " WHERE #{mohair._where.sql()}" if mohair._where?
        sql
    params: (mohair) ->
        params = []

        if mohair._with?
            Object.keys(mohair._with).forEach (key) ->
                params = params.concat asRaw(mohair._with[key]).params()

        params = params.concat mohair._using.params() if mohair._using?
        params = params.concat mohair._where.params() if mohair._where?
        params

module.exports.delete = ->
    Object.create deletePrototype
