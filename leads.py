import re
import sqlite3 as sql
from sqlite3 import Error

'''
	La función crea la conección con la base de datos
'''
class Connect():
	__connector = False
	__table     = ''
	cr          = False
	def __init__(self):

		if not self.__connector:
			try:
				'''
					Se intenta crear la conexión con la base de datos 'isep.db'
				'''
				dbname  = False
				envfile = open(".env", "r")

				while True:
					line = envfile.readline()
					if line.find('dbname') > -1:
						result = re.search('(\"|\').+(\"|\')', line)[0]
						dbname = ''.join( [char for char in result if char not in ('\'','"') ] )
						break

					if not line:
						break

				envfile.close()

				if not dbname:
					raise Warning('Parameter dbname not found!')

				self.__connector = sql.connect(dbname)
				self.cr          = self.__connector.cursor()
			except Error:
				'''
					Mostramos el error si es necesario
				'''
				print(Error)

	'''
		Evaluar si es un numero entero o flotante
	'''
	def __is_number(self, value):
		return isinstance(value, int) or isinstance(value, float)

	'''
		Método para crear tablas recibiendo un nombre de base de datos y una tupla
	'''
	def create_table(self, fields, tablename=None):
		if tablename:
			self.table(tablename)

		if not isinstance(fields, tuple):
			raise Warning('Parameter fields not is a tuple!')

		self.cr.execute('CREATE TABLE IF NOT EXISTS %s (%s)' % (self.__table, ','.join(fields) ) )
		response = self.__connector.commit()
		#self.__connector.close()
		return response

	'''
		Con el método se valida e indica el nombre de la tabla a usar
	'''
	def table(self, tablename):
		if not isinstance(tablename, str):
			raise Warning('Parameter tablename not is string!.')
		self.__table = tablename

	'''
		Crear registro para una tabla indicada
	'''
	def create(self, values, tablename=None):
		if tablename:
			self.table(tablename)

		fields = ','.join([f for f in values.keys()])
		vals   = ','.join([str(v) if self.__is_number(v) else f"'{v}'" for v in values.values()])
		self.cr.execute( f"INSERT INTO {self.__table} ({fields}) VALUES ({vals})" )
		return self.__connector.commit()

	'''
		Remover un registro de la base de datos
	'''
	def unlink(self, id):
		self.cr.execute(f"DELETE FROM {self.__table} WHERE id = {id}")
		return self.__connector.commit()

	'''
		Actualizar registros a través de un id, de parametro
	'''
	def write(self, id, values, tablename=None):
		if tablename:
			self.table(tablename)

		sets  = [f'{key}={v}' if self.__is_number(v) else f'{k}="{v}"' for k, v in values.items()]
		self.cr.execute("UPDATE %s SET %s WHERE id=%s" % (self.__table, ','.join(sets), id ) )
		return self.__connector.commit()

	'''
		Cargar registros de la base de datos
	'''
	def search(self, domain=[], order='', limit=0, tablename=None):
		if tablename:
			self.table(tablename)

		string = ''
		last   = None
		for element in domain:		
			if (isinstance(last, tuple) and isinstance(element,tuple)) or element == '&':
				string += ' AND '
			elif element == '|':
				string += ' OR '

			if isinstance(element, tuple):
				field = element[0]
				cond  = element[1]
				value = element[2]

				if cond == '!=':
					cond = '<>'
				elif cond == 'in' or cond == 'not in':
					value = '(%s)' % ','.join([str(v) for v in value])

				if self.__is_number(value) or cond in ('in','not in'):
					string += f'{field} {cond} {value}'
				else:
					string += f'{field} {cond} "{value}"'

			last = element

		if string != '':
			string = f'WHERE {string} '

		self.cr.execute(f"SELECT * FROM {self.__table} {string}")
		data = self.cr.fetchall()
		
		for l in data:
			pass

		return data


fields = ('id integer PRIMARY KEY', 'name text', 'email text', 'phone text')
db = Connect()
db.table('usuarios')
db.create_table(fields)

db.create({'name':'Jonathan Pérez','email':'jonathan@gmail.com','phone':'5454544'})
db.write(1, {'name':'Miguel', 'phone':'045341444'})

print( db.search( [('id','>', 0)] ) )

#db.unlink(1)


