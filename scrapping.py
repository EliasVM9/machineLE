import mysql.connector
from mysql.connector import Error
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# Configuración de Selenium
chrome_options = Options()
chrome_options.add_argument("--headless")

# Configuración de la conexión a la base de datos MySQL en Azure
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='webscrappingelias22.mysql.database.azure.com',  # Cambia por el nombre de tu servidor
            user='eliasvm9',  # Cambia por tu usuario
            password='Luna123#',  # Cambia por tu contraseña
            database='machinedb2'  # Cambia por el nombre de tu base de datos
        )
        if connection.is_connected():
            print("Conexión exitosa a la base de datos MySQL en Azure")
        return connection
    except Error as e:
        print(f"Error al conectar con MySQL: {e}")
        return None

# Crear tabla para los datos extraídos
def create_table(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS propiedades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            precio VARCHAR(255),
            categoria VARCHAR(50),
            propiedad_id VARCHAR(50) UNIQUE,
            ciudad VARCHAR(100),
            sector VARCHAR(100),
            poblacion_villa_condominio VARCHAR(255),
            dormitorios INT,
            banos INT,
            dimensiones VARCHAR(50),
            descripcion TEXT
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Tabla creada exitosamente o ya existente.")
    except Error as e:
        print(f"Error al crear la tabla: {e}")

# Insertar datos en la base de datos
def insert_data(connection, data):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO propiedades (propiedad_id, precio, categoria, ciudad, sector, poblacion_villa_condominio, dormitorios, banos, dimensiones, descripcion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        precio = VALUES(precio),
        categoria = VALUES(categoria),
        ciudad = VALUES(ciudad),
        sector = VALUES(sector),
        poblacion_villa_condominio = VALUES(poblacion_villa_condominio),
        dormitorios = VALUES(dormitorios),
        banos = VALUES(banos),
        dimensiones = VALUES(dimensiones),
        descripcion = VALUES(descripcion);
        """
        for item in data:
            cursor.execute(insert_query, (
                item['ID'],  # propiedad_id como clave única
                item['Precio'],
                item['Categoría'],
                item['Ciudad'],
                item['Sector'],
                item['Población/Villa/Condominio'],
                item['Dormitorios'],
                item['Baños'],
                item['Dimensiones'],
                item['Descripción']
            ))
        connection.commit()
        print("Datos insertados/actualizados exitosamente en la base de datos.")
    except Error as e:
        print(f"Error al insertar los datos: {e}")

# Obtener el ID más reciente visible en la página principal
def get_latest_id_from_website():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    url = "https://www.inmobiliariamagallanes.com/index.php"
    driver.get(url)

    try:
        # Encuentra el primer enlace a una propiedad en la página principal
        latest_property_element = driver.find_element(By.XPATH, "(//a[contains(@href, 'propiedades-detalles.php?id=')])[1]")
        latest_property_url = latest_property_element.get_attribute("href")
        # Extrae el ID de la URL
        latest_id = int(latest_property_url.split("id=")[-1])
        print(f"Último ID visible en el sitio web: {latest_id}")
        return latest_id
    except Exception as e:
        print(f"Error al obtener el último ID del sitio web: {e}")
        return None
    finally:
        driver.quit()

# Extraer datos de una propiedad
def extract_property_data(driver, property_id):
    url = f"https://www.inmobiliariamagallanes.com/propiedades-detalles.php?id={property_id}"
    driver.get(url)
    time.sleep(2)

    data = {}
    try:
        data['ID'] = property_id
        data['Precio'] = driver.find_element(By.XPATH, "//td[contains(text(), 'Precio')]/following-sibling::td").text.strip()
        data['Categoría'] = driver.find_element(By.XPATH, "//td[contains(text(), 'Categoría')]/following-sibling::td").text.strip()
        data['Ciudad'] = driver.find_element(By.XPATH, "//td[contains(text(), 'Ciudad')]/following-sibling::td").text.strip()
        data['Sector'] = driver.find_element(By.XPATH, "//td[contains(text(), 'Sector')]/following-sibling::td").text.strip()

        # Validar si la categoría o ciudad existen (dato esencial para descartar páginas vacías)
        if not data['Categoría'] or not data['Ciudad']:
            return None

        data['Población/Villa/Condominio'] = driver.find_element(By.XPATH, "//td[contains(text(), 'Población')]/following-sibling::td").text.strip()
        raw_dormitorios = driver.find_element(By.XPATH, "//td[contains(text(), 'Dormitorios')]/following-sibling::td").text.strip()
        data['Dormitorios'] = int(raw_dormitorios) if raw_dormitorios.isdigit() else 0
        raw_banos = driver.find_element(By.XPATH, "//td[contains(text(), 'Baños')]/following-sibling::td").text.strip()
        data['Baños'] = int(raw_banos) if raw_banos.isdigit() else 0
        data['Dimensiones'] = driver.find_element(By.XPATH, "//td[contains(text(), 'Dimensiones')]/following-sibling::td").text.strip()

        descripcion_elementos = driver.find_elements(By.XPATH, "/html/body/div[3]/div/div/div/div[1]/div[2]/div/p")
        data['Descripción'] = "\n".join([elem.text.strip() for elem in descripcion_elementos]) if descripcion_elementos else "Sin descripción"

        return data
    except Exception:
        return None

# Extraer datos de múltiples propiedades
def scrape_properties(connection):
    latest_id = get_latest_id_from_website()  # Detectar el ID más reciente
    if latest_id is None:
        print("No se pudo obtener el último ID del sitio web. Deteniendo.")
        return []

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    properties_data = []
    missing_count = 0

    try:
        print(f"Comenzando a buscar desde el ID {latest_id}...")
        for property_id in range(latest_id + 1, 0, -1):
            print(f"Buscando ID {property_id}...")
            property_data = extract_property_data(driver, property_id)
            if property_data:
                properties_data.append(property_data)
                print(f"Datos extraídos correctamente para ID {property_id}.")
                missing_count = 0
            else:
                print(f"No se encontraron datos para ID {property_id}.")
                missing_count += 1

            # Detener el scraping después de encontrar demasiados IDs vacíos consecutivos
            if missing_count >= 10:  # Puedes ajustar este valor según sea necesario
                print("Se encontraron 10 IDs consecutivos sin datos. Deteniendo scraping.")
                break
    finally:
        driver.quit()

    return properties_data

def close_connection(connection):
    try:
        if connection and connection.is_connected():
            connection.close()
            print("Conexión cerrada exitosamente.")
    except Exception as e:
        print(f"Error al cerrar la conexión: {e}")

# Proceso principal
if __name__ == "__main__":
    connection = create_connection()
    if connection:
        create_table(connection)
        extracted_data = scrape_properties(connection)
        if extracted_data:
            insert_data(connection, extracted_data)
        close_connection(connection)