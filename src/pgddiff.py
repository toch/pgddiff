"""
Copyright (C) 2010  Christophe Philemotte

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

"""Module dbdiff
The module dbdiff allows to compute the difference between two databases. This
is very useful in a replication scenario to investigate which data are
desynchronized or after recovery to check the good resynchronization.

@author: Christophe Philemotte
"""

import psycopg2 as pgsql

class Column (object):
    '''
    Class
    '''
    CONSTRAINT_TYPE = ['NONE', 'UNIQUE', 'CHECK', 'PRIMARY KEY', 'FOREIGN KEY' ]
    
    def __init__(self, name, data_type, is_nullable = False,
                 default = '', constraint = CONSTRAINT_TYPE[0]):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable
        self.default = default
        self.constraint = constraint 

    def is_primary(self):
        return (self.constraint == self.CONSTRAINT_TYPE[3])
    
    def set_primary(self):
        self.constraint = self.CONSTRAINT_TYPE[4]
     
class Table (object):
    def __init__(self, name):
        self.name = name
        self.columns = {}
        self.pkey = None
        self.count = 0
    
    def set_column(self, column):
        if column == None:
            raise ValueError('None value is not permitted')
        if not (column is Column):
            raise TypeError('column is not a Column')
        
        if column.is_primary(): 
            self.pkey = column
        self.columns[column.name] = column
        
    def set_records_count(self, count):
        self.count = count
    
class Database (object):
    '''
    Class
    '''
    def __init__(self):
        self.tables = {}
        self.names = []
        self.idx = {}
        self.pkey = {}
        self.fkeys = {}
        self.done = {}
        self.done['names'] = False
        self.done['idx'] = False
        self.done['pkey'] = False
        self.done['fkeys'] = False
    
    def all_tables_names(self):
        '''
        Method
        '''
        return self.names
    
    def table_count(self, name):# pylint: disable-msg=W0613,R0201
        '''
        
        @param name:
        '''
        return 0

    def table_description(self, name):# pylint: disable-msg=W0613,R0201
        '''
        
        @param name:
        '''
        return ''
    
    def table_indices(self, name):# pylint: disable-msg=W0613,R0201
        '''        
        @param name:
        '''
        return []
    
    def table_primary_key(self, name):
        '''
        
        @param name:
        '''
        return None
    
    def table_foreign_keys(self, name):
        '''
        
        @param name:
        '''
        return None
    
    def primary_keys(self):
        '''
        Method
        '''
        #TODO throw exception if names are not yet build
        for tbl_name in self.names:
            self.pkey[tbl_name] = self.table_primary_key(tbl_name)
        
        self.done['pkey'] = True
    
    def indices(self):
        '''
        Method
        '''
        #TODO throw exception if names are not yet build
        for tbl_name in self.names:
            self.idx[tbl_name] = self.table_indices(tbl_name)
            
        self.done['idx'] = True

    def foreign_keys(self):
        '''
        Method
        '''
        #TODO throw exception if names are not yet build
        for tbl_name in self.names:
            self.fkeys[tbl_name] = self.table_foreign_keys(tbl_name)
    
    def table_primary_key_values(self, name):
        '''
        
        @param name:
        '''
        return []
    
    def table_row_by_pkey(self, name, pkey_value):
        '''
        
        @param name:
        @param pkey:
        '''
        return None
    
    def table_foreign_key_values(self, name):
        '''
        
        @param name:
        '''
        return []
    
    def close(self):
        '''
        Method
        '''
        pass

class PostgreSQL (Database):
    '''
    Class
    '''
    def __init__(self, dsn):
        Database.__init__(self)
        self.connection = pgsql.connect(dsn)
        self.cursor = self.connection.cursor()
        self.done['sequences'] = False
        self.sequences = []
        
    def build_tables(self):
        self.all_tables_names()
        self.primary_keys()
        
        for name in self.names:
            table = Table(name)
            table.set_records_count(self.table_count(name))
            fields = self.table_description(name)
            for field in fields:
                column = Column(field['name'], field['type'], default = field['default'])
                if self.pkey[name] == column.name:
                    column.set_primary()
                table.set_column(column)
            self.tables[name] = table
            
    def all_tables_names(self):
        if not self.done['names']:
            self.cursor.execute("SELECT table_name FROM\
                                information_schema.tables\
                                WHERE table_schema = 'public'\
                                ORDER BY table_name;")
            
            for row in self.cursor:
                self.names.append(row[0])
                
            self.done['names'] = True
        
        return self.names
    
    def all_sequences_names(self):    
        if not self.done['sequences']:    
            self.cursor.execute("SELECT sequence_name FROM\
                                information_schema.sequences\
                                WHERE sequence_schema = 'public'\
                                ORDER BY sequence_name;")
            
            for row in self.cursor:
                self.sequences.append(row[0])
            self.done['sequences'] = True
            
        return self.sequences

    def table_count(self, name):
        '''
        
        @param name:
        '''
        self.cursor.execute("SELECT count(*) FROM " + name + ";")
        return self.cursor.fetchone()[0]

    def sequence_description(self, name):
        self.cursor.execute("SELECT last_value, start_value, increment_by,\
                            max_value, min_value, data_type, numeric_precision\
                            FROM \"" + name + "\" \
                            NATURAL JOIN information_schema.sequences;")
        ret = self.cursor.fetchone()
        decs = dict(last_value = ret[0], start_value = ret[1],
                    increment_by = ret[2], max_value = ret[3],
                    min_value = ret[4], data_type = ret[5],
                    numeric_precision = ret[6])
        
        return decs
    
    def table_description(self, name):
        '''
        
        @param name:
        '''
        self.cursor.execute("SELECT column_name, data_type, column_default\
                            FROM information_schema.columns\
                            WHERE table_name = '" + name + "'\
                            ORDER BY column_name;")
        
        fields = []
        for row in self.cursor:
            fields.append(dict(name = row[0], type = row[1], default = row[2]))     

        return fields

    def table_indices(self, name): 
        '''
        
        @param name:
        '''
        self.cursor.execute("SELECT column_name\
                    FROM information_schema.constraint_column_usage\
                    NATURAL JOIN information_schema.table_constraints\
                    WHERE table_name = '" + name + "';")
        
        idx = []
        for row in self.cursor:
            idx.append(row[0])

        return idx

    def table_primary_key(self, name):
        '''
        
        @param name:
        '''
        self.cursor.execute("SELECT column_name\
                            FROM information_schema.constraint_column_usage\
                            NATURAL JOIN information_schema.table_constraints\
                            WHERE table_name = '" + name + "'\
                            AND constraint_type = 'PRIMARY KEY';")
        res = None
        if self.cursor.rowcount == 1:
            res = self.cursor.fetchone()[0]
        return res


    def table_foreign_keys(self, name):
        '''
        
        @param name:
        '''
        self.cursor.execute("SELECT column_name\
                    FROM information_schema.constraint_column_usage\
                    NATURAL JOIN information_schema.table_constraints\
                    WHERE table_name = '" + name + "'\
                    AND constraint_type = 'FOREIGN KEY';")
        fkey = []
        for row in self.cursor:
            fkey.append(row[0])

        return fkey

    def table_primary_key_values(self, name):
        '''
        
        @param name:
        '''
        #TODO throw exception if names and pkey are not yet build
        #TODO throw exception if table does not have a pkey
        self.cursor.execute("SELECT \"" + self.pkey[name] + "\"\
                            FROM \"" + name + "\";")
        pkey_values = []
        for row in self.cursor:
            pkey_values.append(row[0])
        
        return pkey_values

    def table_row_by_pkey(self, name, pkey_value):
        '''
        
        @param name:
        @param pkey:
        '''
        if isinstance(pkey_value, str) or isinstance(pkey_value, basestring):
            self.cursor.execute("SELECT * FROM \"" + name + "\" WHERE " + 
                                self.pkey[name] + "= \'" + pkey_value + "\';")
        else:
            self.cursor.execute("SELECT * FROM \"" + name + "\" WHERE " + 
                                self.pkey[name] + "= " + str(pkey_value) + ";")
        res = self.cursor.fetchone()
        return res

    def table_foreign_key_values(self, name):
        '''
        @param name:
        '''
        #TODO throw exception if names and pkey are not yet build
        #TODO throw exception if table does not have a pkey
        #TODO todo
        pass

    def close(self):
        '''
        Method
        '''
        self.cursor.close()
        self.connection.close()

if __name__ == '__main__':
    import pprint
    import optparse
    
    usage = "usage: %prog [options] SOURCE TARGET\n\
            SOURCE: database comparison reference, expressed by DSN\n\
            TARGET: compared database, expressed by DSN\n\n\
            DSN: data source name, depend on driver\n\
            \tdbname <host> <port> user password sslmode\n\
            \t\tdbname: database name\n\
            \t\thost: host address (default to UNIX socket)\n\
            \t\tport: port number (default to 5432)\n\
            \t\tuser: user name\n\
            \t\tpassword: password\n\
            \t\tsslmode: SLL mode\n\
            comparison level:\n\
            \t0: table name\n\
            \t1: level 0 + number of records\n\
            \t2: level 1 + column description\n\
            \t3: level 2 + primary key\n\
            \t4: level 3 + primary key value\n\
            \t5: level 4 + record data"
    parser = optparse.OptionParser(usage)
    parser.add_option('-l', '--level',
                      action = 'store', default = '0',
                      dest = 'level', type = "int",
                      help = 'the level of comparison')

    
    (options, args) = parser.parse_args()
    
    if options.level < 0 or options.level > 5:
        print 'The option of comparison level must be an integer in [0,5]'
        parser.print_help()
        exit(1) 
    
    if len(args) != 2:
        print 'One or more arguments are missed'
        print args
        parser.print_help()
        exit(1)
        
    dsn_src = args[0]
    dsn_tgt = args[1]
    
    print "Connection...",
    db_src = PostgreSQL(dsn_src)
    db_tgt = PostgreSQL(dsn_tgt)
    print "Done"
    
    print "Fetch the tables and the sequences...",
    tables_src = db_src.all_tables_names()# pylint: disable-msg=C0103
    tables_tgt = db_tgt.all_tables_names()# pylint: disable-msg=C0103
    sequences_src = db_src.all_sequences_names()
    sequences_tgt = db_tgt.all_sequences_names()
    db_src.primary_keys()
    db_tgt.primary_keys()
    print "Done"
    
    for sequence in sequences_src:
        print "-----------------------------------------------------"
        print "Sequence \'" + sequence + "\'...",
        if sequence in sequences_tgt:
            print "OK"
        else:
            print "KO"
            continue #TODO to be changed
        
        if options.level < 1:
            continue #TODO to be changed
        
        print "\t sequence check: ",
        if db_src.sequence_description(sequence) == db_tgt.sequence_description(sequence):
            print "OK"
        else:
            print "KO"
            pprint.pprint(db_src.sequence_description(sequence))
            pprint.pprint(db_tgt.sequence_description(sequence))
        print "-----------------------------------------------------"
    
    for table in tables_src:
        print "-----------------------------------------------------"
        print "Table \'" + table + "\'...",
        if table in tables_tgt:
            print "OK"
        else:
            print "KO"
            continue #TODO to be changed
        
        if options.level < 1:
            continue #TODO to be changed
        
        print "\trecords count: " + str(db_src.table_count(table)) + " ...",
        if db_src.table_count(table) == db_tgt.table_count(table):
            print "OK"
        else:
            print "KO ",
            print str(db_src.table_count(table) - db_tgt.table_count(table)) + " records miss."
        
        if options.level < 2:
            continue #TODO to be changed

        print "\tdescription ...",
        if db_src.table_description(table) == db_tgt.table_description(table):
            print "OK"
        else:
            print "KO"
            pprint.pprint(db_src.table_description(table))
            pprint.pprint(db_tgt.table_description(table))
            continue #TODO berk
        
        if options.level < 3:
            continue #TODO to be changed

        print "\tprimary key ...",
        print db_src.pkey[table],
        if db_src.pkey[table] == db_tgt.pkey[table]:
            print "OK"
        else:
            print "KO,",
            if db_tgt.pkey[table] == None:
                print "no primary key"
            else:
                print db_tgt.pkey[table]
            continue #TODO to be changed

        if options.level < 4:
            continue #TODO to be changed

        if db_src.pkey[table] != None:
            print "\tprimary key value comparison:"
            pkey_values_src = db_src.table_primary_key_values(table)
            pkey_values_tgt = db_tgt.table_primary_key_values(table)

            for pkey in pkey_values_src:
                print "\t\trecord (" + str(pkey) + ") ...",
                
                if pkey in pkey_values_tgt:
                    print "OK",
                    if options.level < 5:
                        print
                        continue #TODO to be changed
                    print ", record data comparison:",
                    row_src = db_src.table_row_by_pkey(table, pkey)
                    row_tgt = db_tgt.table_row_by_pkey(table, pkey)
                    if row_src == row_tgt:
                        print "OK"
                    else:
                        print "KO",
                        pprint.pprint(row_src)
                        pprint.pprint(row_tgt)
                else:
                    print "KO, record misses."
        print "-----------------------------------------------------"
    
    print "Close connections...",
    db_src.close()
    db_tgt.close()
    print "Done"    
    

