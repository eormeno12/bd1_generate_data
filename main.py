import random
import psycopg2
from faker import Faker
from faker.providers import person, address, date_time, company, lorem, BaseProvider
from decouple import config
from datetime import datetime, timedelta

DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT', default='5432')
DB_SCHEMA = config('DB_SCHEMA', default='public')


# Conéctate a la base de datos
connection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    options=f"-c search_path={DB_NAME},{DB_SCHEMA}"
)

cursor = connection.cursor()

# Configuración de Faker
fake = Faker()
fake.add_provider(person)
fake.add_provider(address)
fake.add_provider(date_time)
fake.add_provider(company)

class PeruPhoneNumberProvider(BaseProvider):
    def phone_number_peru(self):
        return f"{self.random_int(900000000, 999999999)}"
    
class PlasticCategoryProvider(BaseProvider):
    def plastic_category(self):
        categories = ["PET", "HDPE", "LDPE", "PP", "PS", "PVC", "Other"]
        return self.random_element(categories)

class PlasticProductProvider(BaseProvider):
    def plastic_product_name(self):
        prefixes = ["Flexible", "Rigid", "Durable", "High-Performance", "Eco-Friendly"]
        materials = ["Polyethylene", "Polypropylene", "Polyvinyl Chloride", "Polystyrene", "Polyethylene Terephthalate"]
        suffixes = ["Container", "Bottle", "Packaging", "Film", "Sheet", "Tube", "Pipe", "Injection Molded Product", "Extruded Product"]

        product_name = f"{self.random_element(prefixes)} {self.random_element(materials)} {self.random_element(suffixes)}"
        return product_name

class RawMaterialProvider(BaseProvider):
    def raw_material_name(self):
        materials = ["Polyethylene Resin", "Polypropylene Granules", "PVC Polymer", "Polystyrene Beads", "PET Pellets"]
        modifiers = ["High-Quality", "Recycled", "Virgin", "Industrial Grade", "Biodegradable"]

        material_name = f"{self.random_element(modifiers)} {self.random_element(materials)}"
        return material_name
    
fake.add_provider(PeruPhoneNumberProvider)
fake.add_provider(PlasticCategoryProvider)
fake.add_provider(PlasticProductProvider)
fake.add_provider(RawMaterialProvider)

# Función para insertar datos en la tabla Persona
def insert_persona(idx):
    dni = str(idx).zfill(8)

    cursor.execute(
        "INSERT INTO Persona (DNI, Nombre, Celular, CorreoElectronico, Direccion) VALUES (%s, %s, %s, %s, %s) RETURNING DNI",
        (dni, fake.name(), fake.phone_number_peru(), fake.email(), fake.address())
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla Empleado
def insert_empleado(idx):
    dni = insert_persona(idx)

    cursor.execute(
        "INSERT INTO Empleado (DNI) VALUES (%s) RETURNING DNI",
        (dni,)
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla CompradorNatural
def insert_comprador_natural(idx):
    dni = insert_persona(idx)

    cursor.execute(
        "INSERT INTO CompradorNatural (DNI) VALUES (%s) RETURNING DNI",
        (dni,)
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla CompradorJuridico
def insert_comprador_juridico(idx):
    ruc = str(idx).zfill(11)
    cursor.execute(
        "INSERT INTO CompradorJuridico (RUC, Nombre) VALUES (%s, %s) RETURNING RUC",
        (ruc, fake.company())
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla Venta
def insert_venta(empleado_dni, comprador_natural_dni):
    cursor.execute(
        "INSERT INTO Venta (Codigo, PrecioTotal, Fecha, Hora, EmpleadoDNI, CompradorNaturalDNI) VALUES (DEFAULT, %s, %s, %s, %s, %s) RETURNING Codigo",
        (fake.random_number(2), fake.date(), fake.time(), empleado_dni, comprador_natural_dni)
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla Producto
def insert_producto():
    cursor.execute(
        "INSERT INTO Producto (Codigo) VALUES (DEFAULT) RETURNING Codigo"
    )
    return cursor.fetchone()[0]

# Función para insertar datos en la tabla ProductoBase
def insert_producto_base():
    producto_codigo = insert_producto()

    cursor.execute(
        "INSERT INTO ProductoBase (Codigo, Nombre, Stock, PrecioUnitario, Categoria) VALUES (%s, %s, %s, %s, %s) RETURNING Codigo",
        (producto_codigo, fake.plastic_product_name(), fake.random_number(3), fake.random_number(2), fake.plastic_category())
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla ProductoCotizado
def insert_producto_cotizado(producto_base_codigo):
    producto_codigo = insert_producto()

    cursor.execute(
        "INSERT INTO ProductoCotizado (Codigo, NuevoPrecioUnitario, ProductoBaseCodigo) VALUES (%s, %s, %s) RETURNING Codigo",
        (producto_codigo, fake.random_number(2), producto_base_codigo)
    )

    return cursor.fetchone()[0]

# Función para insertar datos en la tabla Lote
def insert_lote(idx):
    currentDateTime = datetime.now() + timedelta(minutes=idx)
    date = currentDateTime.strftime("2000-%m-%d")
    time = currentDateTime.strftime("%H:%M:%S")
    
    cursor.execute(
        "INSERT INTO Lote (Fecha, Hora, CostoTotal) VALUES (%s, %s, %s) RETURNING Fecha, Hora",
        (date, time, fake.random_number(4))
    )
    return cursor.fetchone()

# Función para insertar datos en la tabla MateriaPrima
def insert_materia_prima():
    cursor.execute(
        "INSERT INTO MateriaPrima (Codigo, Nombre, Stock, ValorUnitario) VALUES (DEFAULT, %s, %s, %s) RETURNING Codigo",
        (fake.raw_material_name(), fake.random_number(3), fake.random_number(2))
    )
    return cursor.fetchone()[0]

# Función para insertar datos en la tabla Representa
def insert_representa(dni, ruc):
    cursor.execute(
        "INSERT INTO Representa (CompradorNaturalDNI, CompradorJuridicoRUC) VALUES (%s, %s)",
        (dni, ruc)
    )

# Función para insertar datos en la tabla Tiene
def insert_tiene(venta_codigo, producto_codigo):
    cursor.execute(
        "INSERT INTO Tiene (VentaCodigo, ProductoCodigo, Cantidad) VALUES (%s, %s, %s)",
        (venta_codigo, producto_codigo, fake.random_number(2) + 1)
    )

# Función para insertar datos en la tabla Produce
def insert_produce(producto_base_codigo, lote_fecha, lote_hora):
    cursor.execute(
        "INSERT INTO Produce (ProductoBaseCodigo, LoteFecha, LoteHora, Cantidad) VALUES (%s, %s, %s, %s)",
        (producto_base_codigo, lote_fecha, lote_hora, fake.random_number(3) + 1)
    )

# Función para insertar datos en la tabla Requiere
def insert_requiere(producto_base_codigo, materia_prima_codigo):
    cursor.execute(
        "INSERT INTO Requiere (ProductoBaseCodigo, MateriaPrimaCodigo, Cantidad) VALUES (%s, %s, %s)",
        (producto_base_codigo, materia_prima_codigo, fake.random_number(2) + 1)
    )

# Función para insertar datos en la tabla Pide
def insert_pide(producto_cotizado_codigo, empleado_dni, comprador_natural_dni):
    cursor.execute(
        "INSERT INTO Pide (ProductoCotizadoCodigo, EmpleadoDNI, CompradorNaturalDNI) VALUES (%s, %s, %s)",
        (producto_cotizado_codigo, empleado_dni, comprador_natural_dni)
    )

def generate_data(numRecords):
  # Generar 1k, 10k, 100k, y 1M de datos simulados
  for i in range(numRecords):
      dniEmpleado = insert_empleado(i * 2)
      dniComprador = insert_comprador_natural((i * 2) + 1)

      # Representa y CompradorJuridico
      ruc = insert_comprador_juridico(i)
      insert_representa(dniComprador, ruc)
            

      # Datos para la venta
      producto_base_codigo = insert_producto_base()      
      producto_cotizado_codigo = insert_producto_cotizado(producto_base_codigo)

      insert_pide(producto_cotizado_codigo, dniEmpleado, dniComprador)


      venta_codigo = insert_venta(dniEmpleado, dniComprador)
      isCotizado = random.choice([True, False])

      if(isCotizado):
        insert_tiene(venta_codigo, producto_cotizado_codigo)
      else:
        insert_tiene(venta_codigo, producto_base_codigo)

      lote_fecha, lote_hora = insert_lote(i)
      materia_prima_codigo = insert_materia_prima()

      insert_produce(producto_base_codigo, lote_fecha, lote_hora)
      insert_requiere(producto_base_codigo, materia_prima_codigo)

      print(f"Registro {i+1} de {numRecords}", end="\r")
    

  # Hacer commit de las transacciones
  connection.commit()

  # Cerrar la conexión
  cursor.close()
  connection.close()


def borrar_contenido_todas_las_tablas():
    # Obtener los nombres de todas las tablas en la base de datos
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{DB_SCHEMA}' AND table_type = 'BASE TABLE';
    """)
    tablas = cursor.fetchall()

    # Iterar sobre las tablas y borrar su contenido
    for tabla in tablas:
        nombre_tabla = tabla[0]
        cursor.execute(f"TRUNCATE TABLE {nombre_tabla} RESTART IDENTITY CASCADE")
        print(f"Contenido de la tabla {nombre_tabla} borrado.")

    connection.commit()


def menu_info():
    print("1. Generar datos")
    print("2. Borrar contenido de todas las tablas")
    print("3. Salir")


def select_option():
    option = int(input("Ingrese una opción: "))

    while(option < 1 or option > 3):
        option = int(input("Ingrese una opción válida: "))

    return option


def menu():
    menu_info()


    option = select_option()

    while(option != 3):
        if(option == 1):
            numRecords = int(input("Ingrese el número de registros a generar: "))
            generate_data(numRecords)
        elif(option == 2):
            borrar_contenido_todas_las_tablas()

        menu_info()
        option = select_option()


def main():
    menu()

if __name__ == "__main__":
    main()
