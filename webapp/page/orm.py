# @func: Primary tools for database operations

import logging, aiomysql


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# create a database conextion pool
async def create_pool(loop, **kwargs):
    logging.info('create a database connection pool')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kwargs.get('host', 'localhost'),
        port=kwargs.get('port', '3306'),
        user=kwargs['root'],
        password=kwargs['P@ssW0rd'],
        db=['userList'],
        charset=kwargs.get('charset', 'utf8'),
        autocommit=kwargs.get('autocommit', True),
        maxsize=kwargs.get('maxsize', 10),
        minsize=kwargs.get('minsize', 1),
        loop=loop
    )


# SELECT
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                # get several data
                rows = await cur.fetchmany(size)
            else:
                # get all data
                rows = await cur.fetchall()
            logging.info('Rows returned: %s' % len(rows))
            return rows


# INSERT, DELETE, UPDATE
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.acquire() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


def create_args_string(num):
    return ','.join(['?'] * num)


class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s,%s,%s>' % (self.__class__, __name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name = None, primary_key = False, default = False, ddl = 'varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name = None, primary_key = False, default = False):
        super().__init__(name, 'boolean', primary_key, default)


class IntegerField(Field):
    def __init__(self, name = None, primary_key = False, default = 0):
        super().__init__(name, 'int', primary_key, default)


class FloatField(Field):
    def __init__(self, name = None, primary_key = False, default = 0.0):
        super().__init__(name, 'float', primary_key, default)


class TextField(Field):
    def __init__(self, name = None, primary_key = False, default = False):
        super().__init__(name, 'text', primary_key, default)


class ModelMetaclass(type):
    def __new__(mcs, name, base, attrs):
        # not Model class
        if name == 'Model':
            return type.__new__(mcs, name, base, attrs)
        table_name = attrs.get('__table__', None) or name
        logging.info('Found model: %s (table: %s)' % (name, table_name))
        # get all fields and primary keys
        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('Found mapping: %s->%s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # find primary key
                    if primary_key:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primary_key = k
                else:
                    fields.append(k)
        if not primary_key:
            raise RuntimeError('Primary key not found')
        for k in mappings.keys():
            attrs.pop(k)
        # Log mapping relation of attrs and columns
        attrs['__mappings__'] = mappings
        attrs['__table__'] = table_name
        # Log key value
        attrs['__primary_key__'] = primary_key
        attrs['__fields__'] = fields
        # Construct SQL sentence
        attrs['__select__'] = 'SELECT %s, %s FROM %s' % (primary_key, ','
                                                         .join(fields), table_name)
        attrs['__insert__'] = 'INSERT INTO %s (%s,%s) VALUES (%s)' % (
            table_name, ','.join(fields), primary_key, create_args_string(len(fields) + 1))
        attrs['__update__'] = 'UPDATE %s SET %s WHERE %s = ?' % (table_name,
                                                                 ','.join(map(lambda f: '%s = ?' % (
                                                                             mappings.get(f).name or f), fields)),
                                                                 primary_key)
        attrs['__delete__'] = 'DELETE FROM %s WHERE %s = ?' % (table_name, primary_key)
        return type.__new__(mcs, name, base, attrs)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_default(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('Using default value for %s: %s' % (key, str(value)))
                setattr(self,key,value)
        return value

    # Find objects by WHERE clause
    @classmethod
    async def find_all(cls, where = None, args = None, **kwargs):
        sql = [cls.__select__]
        if where:
            sql.append('WHERE')
            sql.append(where)
        if args is None:
            args = []
        order_by = kwargs.get('order_by', None)
        if order_by:
            sql.append('ORDER BY')
            sql.append(order_by)
        limit = kwargs.get('limit', None)
        if limit is not None:
            sql.append('LIMIT')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rows = await select(' '.join(sql), args)
        return [cls(**row) for row in rows]

    # Find number by select and where
    @classmethod
    async def find_number(cls, select_field, where = None, args = None):
        sql = ['SELECT %s _num_ from %s' % (select_field, cls.__table__)]
        if where:
            sql.append('WHERE')
            sql.append(where)
        rows = await select(' '.join(sql), args, 1)
        if len(rows) == 0:
            return None
        return rows[0]['_num_']

    # Find object by primary key
    @classmethod
    async def find(cls, primary_key):
        rows = await  select('%s WHERE %s = ?' % (cls.__select__, cls.__primary_key__), [primary_key],1)
        if len(rows) == 0:
            return None
        return cls(**rows[0])

    async def save(self):
        args = list(map(self.get_value_or_default, self.__fields__))
        args.append(self.get_value(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('Failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.get_value_or_default, self.__fields__))
        args.append(self.get_value(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('Failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.get_value(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('Failed to remove by primary key: affected rows %s' % rows)