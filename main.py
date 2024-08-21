import mysql.connector
# Funcion que trae la conexion a la base de datos
def get_db():
  db = mysql.connector.connect(
    host="18.116.82.240",
    user="studentsucundi",
    password="mami_prende_la_radi0",
    port="3306",
    database="employees",
    auth_plugin='mysql_native_password',
  )
  return db
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
# Funcion principal que llama a las funciones anteriores
def main():
  emps = read_employees()
  print(emps)
  emp = {
    "emp_no": "666",
    "birth_date": "2003/11/02",
    "first_name": "Santiago",
    "last_name": "Sierra",
    "gender": "M",
    "hire_date": "2021/11/02"
  }
  create_employee(emp)
  emp = read_employee_by_id("666")
  print(emp)
  emp = {
    "emp_no": "666",
    "birth_date": "2003/11/02",
    "first_name": "Santiago",
    "last_name": "Fernandez",
    "gender": "M",
    "hire_date": "2021/11/02"
  }
  update_employee(emp)
  emp = read_employee_by_id("666")
  print(emp)
  delete_employee('666')
main()