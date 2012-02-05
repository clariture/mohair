# mohair

sql query builder for nodejs

### Features

- build common queries easily
- fall back to plain sql for more exotic queries

### Advantages over writing mysql queries by hand

- composability: query parts (for example conditions) can be factored out into functions and reused.
- easier parameter handling: parameters are bound where they are used - no endless parameter lists!

## Usage

### insert

```coffeescript
{insert, sql, params} = require('mohair')()

insert 'project',
    name: 'Amazing Project'
    owner_id: 5
    hidden: false
```

`sql()` returns:

```sql
INSERT INTO project (name, owner_id, hidden) VALUES (?, ?, ?);
```

`params()` returns:

```coffeescript
['Amazing Project', 5, false]
```

#### inserting multiple rows at once

```coffeescript
{insert, sql, params} = require('mohair')()

insert 'project',
    {name: 'First project', hidden: true},
    {name: 'Second project', hidden: false}
```

**Note:**All objects to be inserted must have the same keys.

`sql()` returns:

```sql
INSERT INTO project (name) VALUES (?, ?), (?, ?);
```

`params()` returns:

```coffeescript
['First project', true, 'Second project', false]
```

#### raw values

if you want to insert the result of an sql function you can do so like this:

```coffeescript
{insert, raw, sql, params} = require('mohair')()

insert 'project',
    name: 'Another Project'
    created_on: -> raw 'NOW()'
```

`sql()` returns:

```sql
INSERT INTO project (name, created_on) VALUES (?, NOW());
```

`params()` returns:

```coffeescript
['Another Project']
```

### update

```coffeescript
mohair = {update, where, Is, sql, params} = require('mohair')()

changes =
    name: 'Even more amazing project'
    hidden: true

update 'project', changes, {id: 7}
```
**Note:** the last argument is a query object. see section `Query language` below for details.

`sql()` returns:

```sql
UPDATE project SET name = ?, hidden = ? WHERE id = ?;
```

`params()` returns:

```coffeescript
['Even more amazing project', true, 7]
```

**Note:** you can insert raw values the same way as for inserts.

### select

#### implicit star

```coffeescript
mohair = {select, sql, params} = require('mohair')()

select 'project'
```

`sql()` returns:

```sql
SELECT * FROM project;
```

`params()` returns:

```coffeescript
[]
```

#### explicit column list and where clause

```coffeescript
mohair = {select, where, Is, sql, params} = require('mohair')()

select 'project', ['name', 'id'], -> where {hidden: true}
```

**Note:** `where` takes a query object. see section `Query language` below for details.

`sql()` returns:

```sql
SELECT name, id FROM project WHERE hidden = ?;
```

`params()` returns:

```coffeescript
[true]
```

#### join, groupBy and orderBy

```coffeescript
mohair = {select, leftJoin, groupBy, orderBy, sql, params} = require('mohair')()

select 'project', ['count(task.id) AS taskCount', 'project.*'], ->
    leftJoin 'task', 'project.id' , 'task.project_id'
    groupBy 'project.id'
    orderBy 'project.created_on DESC'
```

**Note:** use `join`, `leftJoin`, `rightJoin`, and `innerJoin` as needed.

`sql()` returns:

```sql
SELECT
    count(task.id) AS taskCount,
    project.*
FROM project
LEFT JOIN task ON project.id = task.project_id
GROUP BY project.id
ORDER BY project.created_on DESC;
```

`params()` returns:

```coffeescript
[]
```

### delete (remove)

```coffeescript
mohair = {where, remove, Is, And, sql, params} = require('mohair')()

remove 'project', {id: 7, hidden: true}
```

**Note:** the last argument is a query object. see section `Query language` below for details.

**Note:** `delete` is a keyword in javascript. use `remove` instead!

`sql()` returns:

```sql
DELETE FROM project WHERE id = ? AND hidden = ?;
```

`params()` returns:

```coffeescript
[7, true]
```

### transactions

```coffeescript
mohair = {transaction, remove, where, Is, sql, params} = require('mohair')()

transaction ->
    remove 'project', {id: 7}
    update 'project', {name: 'New name'}, {id: 8}
```

`sql()` returns:

```sql
START TRANSACTION;
DELETE FROM project WHERE id = ?;
UPDATE project SET name = ? WHERE id = ?;
COMMIT;
```

`params()` returns:

```coffeescript
[7, 'New name', 8]
```
### fallback to raw sql with optional parameter bindings

```coffeescript
{raw, sql, params} = require('mohair')()

raw 'SELECT * FROM project WHERE id = ?;', 7
```

`sql()` returns:

```sql
SELECT * FROM project WHERE id = ?;
```

`params()` returns:

```coffeescript
[7]
```

## Query language

heavily inspired by the [mongo query language](http://www.mongodb.org/display/DOCS/Advanced+Queries)

### query objects

conditions are generated from query objects by using the keys as column names,
binding or calling the values and interspersing 'AND':

```coffeescript
{query, quoted, sql, params} = require('mohair')()

query
    id: 7
    hidden: true
    name: -> quoted 'Another project'
```

`sql()` returns:

```sql
id = ? AND hidden = ? AND name = 'Another project'
```

`params()` returns:

```coffeescript
[7, true]
```

### comparison operators

you can change the default comparison operator '=' as follows:

```coffeescript
{query, quoted, raw, sql, params} = require('mohair')()

query
    id: 7
    name: {$ne: -> quoted 'Another project'}
    owner_id: {$lt: 10}
    category_id: {$lte: 4}
    deadline: {$gt: -> raw 'NOW()'}
    cost: {$gte: 7000}
```

`sql()` returns:

```sql
id = ? AND
name != 'Another project' AND
owner_id < ? AND
category_id <= 4 AND
deadline > NOW() AND
cost >= ?
```

`params()` returns:

```coffeescript
[7, 10, 4, 7000]
```

### $or

the special key `$or` takes an array of query objects and generates a querystring
where only one of the queries must match:

```coffeescript
{query, quoted, raw, sql, params} = require('mohair')()

query
    $or: [
        {id: 7}
        {name: -> quoted 'Another project'}
        {owner_id: 10}
    ]
```

`sql()` returns:

```sql
id = ? OR name = 'Another project' OR owner_id = ?
```

`params()` returns:

```coffeescript
[7, 10]
```

### $and

the special key `$and` takes an array of query objects and generates a querystring
where all of the queries must match.
`$and` and `$or` can be nested:

```coffeescript
{query, quoted, raw, sql, params} = require('mohair')()

query
    id: 7
    $or: [
        {owner_id: 10}
        $and: [
            {cost: {$gt: 500}}
            {cost: {$lt: 1000}}
        ]
    ]
```

`sql()` returns:

```sql
id = ? AND (owner_id = ? OR cost > ? AND cost < ?)
```

`params()` returns:

```coffeescript
[7, 10, 500, 1000]
```

### $in

select rows where column `id` has one of the values: `3, 5, 8, 9`:

```coffeescript
{query, quoted, raw, sql, params} = require('mohair')()

query
    id: {$in: [3, 5, 8, 9]}
```

`sql()` returns:

```sql
id IN (?, ?, ?, ?)
```

`params()` returns:

```coffeescript
[3, 5, 8, 9]
```

## Use it with node-mysql

```coffeescript

mysql = require 'mysql'

client = mysql.createClient
    user: 'root'
    password: 'root'

{insert, sql, params} = require('mohair')()

insert 'project',
    name: 'Amazing Project'
    owner_id: 5
    hidden: false

client.query sql(), params(), (err, result) ->
    console.log result
```
