# Base de datos 1

Script para generar los datos simulados.

## Requisitos

- Python 3.x
- PostgreSQL

## Configuración del Entorno Virtual (Virtual Environment)

1. Crea un entorno virtual:

    ```bash
    python -m venv venv
    ```

2. Activa el entorno virtual:

    - En Windows:

        ```bash
        venv\Scripts\activate
        ```

    - En Linux/Mac:

        ```bash
        source venv/bin/activate
        ```

## Instalación de Dependencias

Instala las dependencias del proyecto:

```bash
pip install -r requirements.txt
```

## Configuración de Variables de Entorno

1. Crea un archivo .env en la raíz del proyecto.

2. Agrega las siguientes variables de entorno con sus valores correspondientes (este es un ejemplo):

```bash
    DB_NAME=nombre_de_la_base_de_datos
    DB_USER=nombre_de_usuario
    DB_PASSWORD=contraseña
    DB_HOST=127.0.0.1
    DB_PORT=5432
    DB_SCHEMA=public
```

3. Guarda el archivo .env.

## Ejecución del Proyecto

Ejecuta el script principal:

```bash
python main.py
```

Este comando iniciará la aplicación y realizará las operaciones necesarias.

