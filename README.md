# Taller de Big Data: Pipeline ETL Distribuido con PySpark y MySQL

Proyecto académico de construcción y orquestación de un flujo de datos (pipeline ETL) que extrae información transaccional masiva, la procesa de forma distribuida en memoria mediante Apache Spark (PySpark) y persiste tanto el lago de datos crudo como los resultados analíticos en un motor relacional MySQL.

---

## Integrantes
- **Guillermo Andres Minero Alfaro**: Diseño de arquitectura ETL, configuración de SparkSession optimizada, procesamiento distribuido y modelado relacional en MySQL.
- **Ruben Armando Vigil Mejia**: Evaluación analítica, validación de persistencia con sentencias preparadas y análisis de negocio sobre las métricas.

---

## Contenido del Proyecto
El pipeline implementado en [main.ipynb](main.ipynb) abarca cuatro fases de ingeniería de datos:

1. **Configuración del Entorno y Extracción**:
   - Inicialización de `SparkSession` optimizada para procesamiento local multinúcleo (`local[*]`, AQE activado, particiones de shuffle sintonizadas a 8, serializador Kryo).
   - Generación interna determinista (`seed=42`) de un dataset masivo de transacciones e-commerce (**60,600 registros**) con inyección controlada de ruido (valores nulos, registros duplicados, inconsistencias en formato de texto y montos negativos/cero).
   - Ingesta distribuida aplicando un esquema formal estricto (`StructType`) para evitar el costo computacional de inferencia.

2. **Procesamiento Distribuido con PySpark**:
   - Perfilado y diagnóstico de calidad de datos mediante operaciones paralelas en una sola pasada.
   - Limpieza y estandarización: deduplicación exacta y por clave de negocio, normalización tipográfica (`trim`, `initcap`), casteos temporales y eliminación/imputación controlada de nulos.
   - Columnas calculadas de negocio: cálculo del subtotal, aplicación del IVA del 13 % (El Salvador), `total_con_impuestos`, extracción de hora/día de la semana y segmentación dinámica de clientes por ticket de compra.
   - Filtros avanzados multicondición y caching en memoria (`.cache()`) para reutilización de planes lógicos de ejecución.
   - Agregaciones masivas con `groupBy` y funciones de ventana analítica (`Window.partitionBy()`) para calcular ticket promedio, unidades vendidas, ingresos totales y porcentaje de participación por categoría y hora.

3. **Persistencia en MySQL**:
   - Conexión segura Python ↔ MySQL mediante `mysql-connector-python` con protocolo SSL y credenciales aisladas del código.
   - Creación automática (DDL) de las tablas relacionales (`metricas_ventas_hora`, `metricas_ventas_categoria` y `transacciones_raw`).
   - Carga transaccional e idempotente mediante sentencias preparadas con sintaxis `ON DUPLICATE KEY UPDATE` para garantizar atomicidad y prevenir inyecciones SQL.
   - Persistencia completa del dataset original (60,600 filas) mediante inserción optimizada por bloques (`batch processing`).

4. **Validación de la Persistencia y Conciliación**:
   - Consultas `SELECT` finales desde MySQL retornadas a DataFrames de Pandas para verificación visual.
   - Conciliación matemática automática: contraste de conteos y sumas de ingresos entre PySpark y MySQL para certificar **cero pérdida de datos** (diferencia < $0.05).

---

## Tecnologías Utilizadas
- **Python 3.13**
- **Apache Spark (PySpark 4.x)**: Motor distribuido de cómputo en memoria.
- **MySQL 8.0**: Motor relacional de base de datos para la persistencia analítica.
- **mysql-connector-python**: Driver oficial de conexión segura Python/MySQL.
- **Pandas & Matplotlib**: Extracción de resúmenes agregados y visualización de tendencias de negocio.

---

## Ejecución del Pipeline
1. **Prerrequisitos**:
   - Tener instalado Python 3.10+ y Java JDK 17 o superior en el `PATH` del sistema.
   - Servicio MySQL activo en el puerto `3306`.
2. **Base de Datos**:
   - Si no existe el usuario ni la base de datos, ejecutar previamente el **Bloque 1** del archivo [`script_mysql_bigdata.sql`](script_mysql_bigdata.sql) como usuario administrador `root`:
     ```sql
     CREATE DATABASE IF NOT EXISTS bigdata_pipeline CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
     CREATE USER IF NOT EXISTS 'spark_etl'@'localhost' IDENTIFIED BY 'SparkEtl#2025';
     GRANT ALL PRIVILEGES ON bigdata_pipeline.* TO 'spark_etl'@'localhost';
     FLUSH PRIVILEGES;
     ```
3. **Ejecución del Notebook**:
   - Abrir y ejecutar todas las celdas de [main.ipynb](main.ipynb).
   - El archivo genera el dataset de forma autónoma, ejecuta el pipeline en Spark y persiste las 3 tablas en MySQL.

---

## Análisis de Negocio
A partir de los datos procesados se desprenden los siguientes hallazgos estratégicos:
1. **Franjas Horarias Pico**:
   - El volumen de transacciones presenta dos picos pronunciados: el **mediodía (12:00 h con 3,533 ventas)** y el **horario estelar nocturno (20:00 h con 3,793 ventas)**, siendo este último el intervalo de mayor rentabilidad del día.
2. **Dominancia de Categorías**:
   - La categoría de **Electrónica** concentra el **53.9 % de los ingresos totales**, a pesar de representar únicamente alrededor del 20 % del volumen de órdenes de compra, debido a su elevado ticket promedio.
3. **Calidad de Datos en la Ingesta**:
   - La etapa de limpieza distribuida filtró y descartó aproximadamente el 5.5 % de registros inconsistentes (montos nulos, duplicados y cantidades negativas), protegiendo la veracidad de los reportes financieros corporativos.
