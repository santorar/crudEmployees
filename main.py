import mysql.connector
import pymongo

# Funcion que trae la conexion a la base de datos
def get_db():
  try:
    db = mysql.connector.connect(
      host="172.31.25.14",
      user="studentsucundi",
      password="mami_prende_la_radi0",
      port="3306",
      database="employees",
    )
    return db
  except Exception as e:
    print("Error de conexion a la base de datos")

# Funcion para cerrar la conexion a la base de datos para no dejar conexiones abiertas
def close_db(db):
  db.close()

# Funcion que trae los ultimos 10 empleados de la base de datos
def read_employees():
  db = get_db()
  cursor = db.cursor()
  cursor.execute("SELECT * FROM employees LIMIT 10")
  result = cursor.fetchall()
  close_db(db)
  return result

# Funcion que trae un empleado por su id(emp_no)
def read_employee_by_id(id):
  db = get_db()
  cursor = db.cursor()
  cursor.execute("SELECT * FROM employees WHERE emp_no = %s", (id,))
  result = cursor.fetchone()
  close_db(db)
  return result

# Funcion para crear un empleado en la base de datos
def create_employee(employee):
  db = get_db()
  cursor = db.cursor()
  cursor.execute("INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date) VALUES (%s, %s, %s, %s, %s, %s)", (employee["emp_no"],employee["birth_date"], employee["first_name"], employee["last_name"], employee["gender"], employee["hire_date"]))
  db.commit()
  close_db(db)

# Funcion para actualizar un empleado en la base de datos por su id(emp_no)
def update_employee(employee):
  db = get_db()
  cursor = db.cursor()
  cursor.execute("UPDATE employees SET emp_no = %s, birth_date = %s, first_name = %s, last_name = %s, gender = %s, hire_date = %s WHERE emp_no = %s", (employee["emp_no"],employee["birth_date"], employee["first_name"], employee["last_name"] , employee["gender"], employee["hire_date"], employee["emp_no"]))
  db.commit()
  close_db(db)

# Funcion para eliminar un empleado de la base de datos por su id(emp_no)
def delete_employee(id):
  db = get_db()
  cursor = db.cursor()
  cursor.execute("DELETE FROM employees WHERE emp_no = %s", (id,))
  db.commit()
  close_db(db)

def get_db_mongo():
  try:
    client = pymongo.MongoClient("mongodb+srv://estractor179:Lpm5v4agTrz9cNzW@cluster0.lirj4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    database = client['sample_mflix']
    return database
  except Exception as e:
    print("Error de conexion a la base de datos")

def create_user_collection_mongo():
  db = get_db_mongo()
  collection = db['users']
  return collection
def ping_mongo():
  db = get_db_mongo()
  db.command('ping')
  print("Connected to MongoDB")
def create_user_mongo(user):
  collection = create_user_collection_mongo()
  collection.insert_one(user)

def read_users_mongo():
  collection = create_user_collection_mongo()
  result = collection.find()
  return result
def read_user_by_id_mongo(id):
  collection = create_user_collection_mongo()
  result = collection.find_one({"_id": id})
  return result
def update_user_mongo(user):
  collection = create_user_collection_mongo()
  collection.update_one({"_id": user["_id"]}, {"$set": user})
def delete_user_mongo(id):
  collection = create_user_collection_mongo()
  collection.delete_one({"_id": id})

# Funcion principal que llama a las funciones anteriores
def main():
  create_user_mongo({
    "_id": "666",
    "name": "Santiago",
    "email": "estractor179@gmail.com",
    "password": "123456",
    "age": 18
  })

  print(read_user_by_id_mongo("666"))
  update_user_mongo({
    "_id": "666",
    "name": "Santiago",
    "email": "santorar179@gmail.com",
    "password": "123456",
    "age": 18
  })
  print(read_user_by_id_mongo("666"))
  delete_user_mongo("666")
  # emps = read_employees()
  # print(emps)
  # emp = {
  #   "emp_no": "666",
  #   "birth_date": "2003/11/02",
  #   "first_name": "Santiago",
  #   "last_name": "Sierra",
  #   "gender": "M",
  #   "hire_date": "2021/11/02"
  # }
  # create_employee(emp)
  # emp = read_employee_by_id("666")
  # print(emp)
  # emp = {
  #   "emp_no": "666",
  #   "birth_date": "2003/11/02",
  #   "first_name": "Santiago",
  #   "last_name": "Fernandez",
  #   "gender": "M",
  #   "hire_date": "2021/11/02"
  # }
  # update_employee(emp)
  # emp = read_employee_by_id("666")
  # print(emp)
  # delete_employee('666')


if __name__ == "__main__":
  main()