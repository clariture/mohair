criterion = require 'criterion'

actions = require './actions'

{isRaw, extend} = require './util'

rawPrototype =
    sql: ->
        return @_sql unless @_params

        i = -1
        params = @_params

        @_sql.replace /\?/g, ->
            i++
            if Array.isArray params[i]
                (params[i].map -> "?").join ", "
            else if isRaw params[i]
                params[i].sql()
            else
                "?"

    params: ->
        if @_params
            params = []
            @_params.forEach (c) -> params = params.concat if isRaw c then c.params() else c
            params

module.exports =
    raw: (sql, params...) ->
        object = Object.create rawPrototype
        object._sql = sql
        object._params = params
        object

    fluent: (key, value) ->
        object = Object.create @
        object[key] = value
        object

    _escape: (string) -> string
    _action: actions.select '*'
    _joins: []

    insert: (data) ->
        @fluent '_action', actions.insert data

    escape: (arg) ->
        @fluent '_escape', arg

    select: ->
        @fluent '_action', actions.select.apply null, arguments

    delete: ->
        @fluent '_action', actions.delete()

    update: (updates) ->
        @fluent '_action', actions.update updates

    join: (sql, params...) ->
        join = @raw sql, params...
        @fluent '_joins', @_joins.concat join

    with: (arg) ->
        unless ('object' is typeof arg) and Object.keys(arg).length isnt 0
            @fluent '_with', null
        else
            obj = if @_with? then extend {}, @_with else {}
            for key, val of arg
                extend obj, val._with if val._with?
                obj[key] = if val.with? then val.with() else val
            @fluent '_with', obj
    group: (arg) ->
        @fluent '_group', arg
    order: (sql, params...) ->
        order = @raw sql, params...
        @fluent '_order', order
    limit: (arg) ->
        @fluent '_limit', parseInt(arg, 10)
    offset: (arg) ->
        @fluent '_offset', parseInt(arg, 10)
    distinct: () ->
        @fluent '_distinct', true

    table: (table) ->
        @fluent '_table', table
    attributes: (attributes) ->
        @fluent '_attributes', attributes

    where: (args...) ->
        where = criterion args...
        @fluent '_where', if @_where? then @_where.and(where) else where

    having: (args...) ->
        having = criterion args...
        @fluent '_having', if @_having? then @_having.and(having) else having

    mixin: (fn, args...) ->
        m = fn.apply @, args
        unless m
            throw new Error 'mixin must be called with a function that returns a value'
        m

    from: (sql, params...) ->
        from = @raw sql, params...
        @fluent '_from', from

    sql: ->
        @_action.sql @

    params: ->
        @_action.params @
