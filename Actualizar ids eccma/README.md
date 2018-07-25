[Story 3471](https://kontenix.tpondemand.com/entity/3471-eotd-actualiza-terminos-sin-id-a) <br/>
Este proceso se definió para actualizar id's de eOTD que envía ECCMA para terminología y conceptos que han sido creados en el sistema usando los botones disponibles en el diccionario corporativo.   Son términos o conceptos que se crean sin id's y por lo tanto este proceso es para agregarles los id's que creó ECCMA y que agregaron a su eOTD.


# Proceso que se debe de seguir una sola vez durante todo el procedimiento.

Como primer y *único* paso se debe de crear las tablas necesarias.
Esto se crea ejecutando el archivo de nombre: xx_utils.sql

- [xx_utils.sql](/uploads/c638e0c40cfa332c096651e198a91c08/xx_utils.sql)

Para saber si las tablas anteriormente mencionadas ya existen en la base de datos, se puede mirar el árbol desde la ventana de pgAdmin para confirmar la existencia de éstas tablas.

Ó también ejecutando el siguiente query desde el editor del pgAdmin para saber si existen las tablas mencionadas:

- `SELECT * FROM xx_eccma_new_ids;`
- `SELECT * FROM xxdata_not_found;`


# Procesos recurrentes
## 1 - Realizar backup de la base de datos

#### 1.1 Conectar a servidor mediante SSH
Abrir una terminal, ubicar el directorio de la llave ".pem", tener a la mano la URL donde está corriendo la instancia de Staging o la instancia de Producción y ejecutar el siguiente comando para poder conectarse.

Conexión a Staging: ```ssh -i kontenixserver.pem ubuntu@[url_de_la_instancia]```

Conexión a Producción: `ssh -i kontenixserver.pem ubuntu@ec2-52-7-88-147.compute-1.amazonaws.com`

#### 1.2 Realizar backup
Se puede realizar un backup de la base de datos desde la aplicación o ejecutando el script `make_backup.sh`, conectado al servidor a nivel del sistema operativo:

Se ejecuta de la siguiente manera posicionándose en el directorio raíz de Ubuntu, ya sea para Producción o para Staging (el script ya está configurado):  ```./make_backup.sh```

Si llegara a pedir contraseña, son las siguientes respectivamente.

- Contraseña para BD Staging: `postgres`
- Contraseña para BD Producción: `=wvdi2ULT9*Bak`


## 2 Subir el archivo CSV para poder cargar la información a la tabla `xx_eccma_new_ids`

El archivo se debe de llamar: `xx_eccma_new_ids.csv`.

Para subir el archivo csv es necesario conectarse al servidor mediante FTP, lo recomendable es ponerlo en el directorio de Ubuntu, tanto para la instancia de Staging como para la de Producción.

[xx_eccma_new_ids.csv](/uploads/ca39ca8d94b2d378dcfe50410a6fb696/xx_eccma_new_ids.csv)


A continuación se muestra un ejemplo de la estructura del archivo csv.

| eccma_concept_id   | eccma_term_id      | term_content            | eccma_definition_id | definition_content                 | eccma_abbreviation_id | abbreviation_content | eccma_language_id | language_name | eccma_organization_id | organization_name | organization_mail_address |
|--------------------|--------------------|-------------------------|---------------------|------------------------------------|-----------------------|----------------------|-------------------|---------------|-----------------------|-------------------|---------------------------|
| 0161-1#02-xxxxxx#6 | 0161-1#TM-xxxxx#11 | EJEMPLO MOSTRAR GENERAL | 0161-1#DF-xxxxxx#12 | EJEMPLO QUE SE MOSTRARÁ EN GENERAL | 0161-1#AB-xxxxxx#10   | EJEMOGE              | 0161-1#LG-xxxxxx#10 | Spanish       | 0161-1#OG-482897#3    | Grupo Lamosa      |                           |


## 3 Cargar la tabla `xx_eccma_new_ids` con los datos del archivo csv

Es necesario conectarse a `psql` a la respectiva base de datos para poder ejecutar los comandos necesarios.

### 3.1 Conexión a psql Staging

_Conexión a la base de datos de Staging:_

- `Usuario: postgres`
- `Contraseña: postgres`
- `Base de datos (Staging): kontenix_development`

Conexión a mediante terminal en Staging:
`psql -h localhost -U postgres -d kontenix_development`


### 3.1 Conexión a psql Producción

_Conexión a la base de datos de Producción:_

- `Usuario: postgres`
- `Contraseña: =wvdi2ULT9*Bak`
- `Servidor RDS: kontenix.cuxppvqpu0tq.us-east-1.rds.amazonaws.com`
- `Base de datos (Producción): kontenix`

Conexión mediante terminal en Producción:
`psql -h kontenix.cuxppvqpu0tq.us-east-1.rds.amazonaws.com -U postgres -d kontenix`

### 3.2 Truncar tabla xx_eccma_new_ids
Si es que ya se ha ejecutado este procedimiento, es necesario primero truncar la tabla: xx_eccma_new_ids.

Conectado a psql ejecutando la siguiente instrucción:
`TRUNCATE xx_eccma_new_ids;`

### 3.3 Ejecutar comando COPY para Staging o Producción
Comando para copiar los datos del archivo csv a la tabla `xx_eccma_new_ids` una vez conectado a `psql` sea para Producción o para Staging:

Comando:
`\COPY xx_eccma_new_ids FROM '/home/ubuntu/xx_eccma_new_ids.csv' DELIMITER ',' CSV HEADER;`

> Nota: Se ejecuta el comando `\COPY` a nivel de la terminal del S.O. porque en Producción el usuario postgres no tiene los permisos necesarios para ejecutar este comando COPY.


Comandos útiles de postgres:
```
Salir de psql en Postgres:  \q 
Mostrar las tablas existes de la base de datos conectada: \dt
Mostrar los usuarios existentes: \du
Mostrar todos los objetos de la base de datos conectada: \d
Listar las bases de datos existentes: \l
Cambiar de base de datos: \c [nombre_BD]
Salir de la pantalla si se ejecutó algún comando anterior: q
```


## 4 Ejecución del script
Una vez que se han realizado los pasos anteriores, se procede a ejecutar el procedimiento anónimo del archivo llamado: `xx_update_ids_eccma.sql` , lo más recomendable es copiar y pegar el código en un editor pgAdmin, conectado a la instancia correspondiente.

[xx_update_ids_eccma.sql](/uploads/ecf714ba25536741922f11f4be82aab0/xx_update_ids_eccma.sql)

---

# Pasos para la recuperación de la base de datos

Deben seguirse si es que llegara a ocurrir un error con la integridad de la base de datos.

## 1 Verificar que se tiene un "dump"
Es importante que se verifique si se tiene un dump de la última versión de la base de datos a la mano.

## 2 Borrar la base de datos que se desea restaurar
Para continuar con este paso es necesario primero conectarse a `psql` para ejecutar los comandos correspondientes.

### 2.1 Conexión a psql Staging

Conexión a mediante terminal en Staging:
`psql -h localhost -U postgres`

Contraseña: `postgres`

### 2.1 Conexión a psql Producción

Conexión mediante terminal en Producción: `psql -h kontenix.cuxppvqpu0tq.us-east-1.rds.amazonaws.com -U postgres`

Contraseña: `=wvdi2ULT9*Bak`

### 2.2 Borrar base de datos
Una vez que se está conectado a `psql` con el usuario `postgres`, no con la base de datos, se ejecuta la siguiente instrucción.

#### Instrucción para Staging
Instrucción: `DROP DATABASE kontenix_development;`

#### Instrucción para Producción
Instrucción: `DROP DATABASE kontenix;`

## 3 Crear la base de datos
Manteniendo la conexión a `psql` es necesario crear la base de datos.

### 3.1 Instrucción para crear BD Staging
- `CREATE DATABASE kontenix_development;`

### 3.1 Instrucción para crear BD Producción
- `CREATE DATABASE kontenix;`

## 4 Restaurar base de datos
Una vez que ya está creada la base de datos se procede a restaurarla.

> Nota: Se puede verificar si la base de datos está creada ejecutando la instrucción desde psql:  `\l`

Para restaurar la base de datos se debe de posicionar o ubicar el dump con el que se desea restaurar la BD.

Desde el prompt normal de la terminal de Linux se debe ejecutar el siguiente comando (ejemplo si el dump estuviera en directorio raíz):

### 4.1 Restaurar BD Staging

Instrucción: `pg_restore -h localhost -U postgres -d kontenix_development -v {NOMBRE_ARCHIVO.dump}`

Contraseña: `postgres`

### 4.1 Restaurar BD Producción
Instrucción: `pg_restore -h kontenix.cuxppvqpu0tq.us-east-1.rds.amazonaws.com -U postgres -d kontenix -v {NOMBRE_ARCHIVO.dump}`

Contraseña: `=wvdi2ULT9*Bak`

## 5 Verificar los datos restaurados
Se puede hacer una simple verificación de los datos restaurados ejecutando el siguiente Query desde pgAdmin.

Query:
```
SELECT t.terminology_class, ct.name, count(1)
  FROM terminologicals t,
        concepts C,
        concept_types ct
 WHERE 1 = 1
   -- Match
   AND c.concept_type_id = ct.id
   AND t.concept_id = c.id
   -- Other conditions
   AND t.is_deprecated IS FALSE
   AND t.terminology_class IN ('term','definition')
 GROUP BY t.terminology_class, ct.name
 ORDER BY 1,2;
```

--- 
## Datos requeridos para la conexión a pgAdmin

## Datos para Staging
#### Pestaña general
- Name: Es el nombre que se le quiera dar a la conexión

#### Pestaña Connection
- Host name/address (URL de la instancia): ec2-54-166-60-217.compute-1.amazonaws.com
- Port: 5432
- Maintenance database: postgres
- username: postgres

#### Pestaña SSL
- SSL mode: Prefer
- Client certificate key: Ruta de la llave ".pem" en la máquina local.

## Datos para Producción
#### Pestaña general
- Name: Es el nombre que se le quiera dar a la conexión

#### Pestaña Connection
- Host name/address (URL de la instancia RDS): kontenix.cuxppvqpu0tq.us-east-1.rds.amazonaws.com
- Port: 5432
- Maintenance database: kontenix
- Username: postgres
- Password: =wvdi2ULT9*Bak

#### Pestaña SSL
- SSL mode: Prefer
- Client certificate key: Ruta de la llave ".pem" en la máquina local.

![pestaña_general](/uploads/062188858f7db524ca18231daa53f04b/pestaña_general.JPG)

![pes_conecction](/uploads/f5128e9b5519ef37d848174b921811d8/pes_conecction.JPG)

![pes_ssl](/uploads/2f121d8199bc494789ff553f8f6f802a/pes_ssl.JPG)