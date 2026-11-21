# BancoCloud · DSY1107 · Evaluación Parcial 5

Aplicación bancaria con React, TypeScript y microservicios Java 21 con Spring Boot. Un **Cliente** solicita cuentas y consulta su estado. Un **Empleado** revisa solicitudes, habilita cuentas y administra la mensajería. Amazon Cognito emite el JWT que el frontend adjunta y el backend valida.

## Arquitectura

- **AWS.** Amplify aloja el frontend. API Gateway dirige las peticiones a `banco-bff` en EC2. `cuentas-service` y `solicitudes-service` utilizan MySQL en RDS.
- **RabbitMQ.** Mantiene el flujo operativo de solicitudes. El panel de Empleado permite enviar un mensaje de prueba y administrar colas, exchanges y bindings. Los consumidores registran errores y derivan los mensajes fallidos a su DLQ.
- **Kafka.** Las solicitudes creadas y las cuentas habilitadas generan eventos. `kafka-service` los consume, registra métricas en MySQL y expone la administración a través del BFF. Esta integración se verificó en Docker local y queda pendiente habilitarla en AWS.

| Tópico Kafka | Grupo consumidor | Retención |
|---|---|---:|
| `bancocloud.solicitudes.creadas.v1` | `bancocloud-analitica-solicitudes-v1` | 7 días |
| `bancocloud.cuentas.habilitadas.v1` | `bancocloud-analitica-cuentas-v1` | 30 días |

Cada tópico usa **3 particiones**, **3 réplicas**, `min.insync.replicas=2` y `cleanup.policy=delete`. Las particiones reparten el consumo y los períodos de retención permiten revisar una semana de solicitudes y un mes de altas de cuentas.

## Cómo levantar BancoCloud en local

Necesitas Docker Compose, Java 21, Node.js 22.12 o superior y una cuenta de Cognito. La consulta administrativa de usuarios requiere credenciales AWS con acceso al pool.

### 1. Levanta Docker

En la raíz del proyecto, crea `.env` a partir de [`.env.example`](.env.example) si todavía no existe. Define allí el usuario y la contraseña de RabbitMQ. Después ejecuta:

~~~powershell
docker compose --profile rabbitmq-cluster up -d --wait rabbitmq-cluster-1 rabbitmq-cluster-2 rabbitmq-cluster-3
docker compose --profile kafka up -d --wait --wait-timeout 240 kafka-ui
docker start bancocloud-mysql-local
~~~

El primer comando inicia los tres nodos de RabbitMQ. El segundo inicia ZooKeeper, los tres brokers y [Kafka UI](http://localhost:8085/). El último prende la base de pruebas en el puerto `3307`.

<details>
<summary>Si todavía no tienes el contenedor MySQL local</summary>

En la raíz, crea `.env.mysql-local` con los valores de prueba de abajo. Elige tus propias contraseñas.

~~~dotenv
MYSQL_DATABASE=bancocloud_local
MYSQL_USER=usuario_local
MYSQL_PASSWORD=reemplaza_por_una_clave_local
MYSQL_ROOT_PASSWORD=reemplaza_por_otra_clave_local
~~~

Ejecuta este comando una sola vez en lugar de `docker start bancocloud-mysql-local`:

~~~powershell
docker run -d --name bancocloud-mysql-local -p 127.0.0.1:3307:3306 --env-file .env.mysql-local -v bancocloud-mysql-local-data:/var/lib/mysql mysql:8.4
~~~

</details>

### 2. Guarda la configuración del backend

Crea un solo archivo `application-local.properties` en la **raíz del proyecto**, al lado del Compose. Sustituye los cuatro valores de usuario y contraseña por los de tus contenedores locales. Los de RabbitMQ son los mismos que pusiste en `.env`.

~~~properties
DB_URL=jdbc:mysql://127.0.0.1:3307/bancocloud_local?sslMode=DISABLED&allowPublicKeyRetrieval=true
DB_USERNAME=tu_usuario_mysql
DB_PASSWORD=tu_clave_mysql
RABBITMQ_USERNAME=tu_usuario_rabbitmq
RABBITMQ_PASSWORD=tu_clave_rabbitmq
RABBITMQ_PORT=5673
RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15673
KAFKA_SOLICITUDES_ENABLED=true
KAFKA_CUENTAS_ENABLED=true
server.address=127.0.0.1
~~~

Git ignora los archivos de configuración local. Los servicios comparten este archivo y usan los brokers Kafka locales en `9092–9094`.

### 3. Inicia los microservicios

Abre una terminal por módulo y ubícala dentro de su carpeta. Ejecuta **las mismas dos líneas** en cada una y déjalas abiertas:

~~~powershell
$env:SPRING_CONFIG_ADDITIONAL_LOCATION = 'file:../application-local.properties'
.\mvnw.cmd spring-boot:run
~~~

| Carpeta | Comprobación |
|---|---|
| `cuentas-service` | [8081](http://localhost:8081/actuator/health) |
| `solicitudes-service` | [8082](http://localhost:8082/actuator/health) |
| `kafka-service` | [8083](http://localhost:8083/actuator/health) |
| `banco-bff` | [8080](http://localhost:8080/actuator/health) |

Cada enlace debe mostrar `UP`. Si un puerto ya está en uso, revisa su enlace antes de arrancar otra instancia.

### 4. Abre el frontend

En otra terminal, desde la raíz del proyecto:

~~~powershell
Set-Location banco-frontend
$env:VITE_API_BASE_URL = 'http://localhost:8080'
npm.cmd ci
npm.cmd run dev -- --strictPort
~~~

Entra a [BancoCloud local](http://localhost:5173/). Para comprobar el build, ejecuta `npm.cmd run build` desde `banco-frontend`.

## Guía de revisión de la Evaluación Parcial 5

| Indicador | Dónde revisarlo |
|---|---|
| **Configuración del cliente Kafka (12%)** | [Docker Compose](docker-compose.yml) y propiedades de [solicitudes](solicitudes-service/src/main/resources/application.properties), [cuentas](cuentas-service/src/main/resources/application.properties) y [Kafka](kafka-service/src/main/resources/application.properties). Incluyen brokers, serialización, grupos y confirmación de mensajes. |
| **DTO, productores y consumidores (12% + 14% + 14%)** | [Productor de solicitudes](solicitudes-service/src/main/java/cl/duoc/solicitudes_service/messaging/kafka/SolicitudKafkaPublisher.java) y [productor de cuentas](cuentas-service/src/main/java/cl/duoc/cuentas_service/messaging/kafka/CuentaKafkaPublisher.java). Guardan el evento tipado en una tabla de pendientes junto con el cambio de negocio y lo envían como JSON. Los [listeners](kafka-service/src/main/java/cl/duoc/kafka_service/eventos/ConsumidoresKafka.java) delegan el procesamiento a [AnaliticaKafka](kafka-service/src/main/java/cl/duoc/kafka_service/analitica/AnaliticaKafka.java), que deduplica por `eventId`. |
| **Administración de tópicos y grupos (6% + 6%)** | Panel **Empleado → Kafka** y [API administrativa](kafka-service/src/main/java/cl/duoc/kafka_service/admin/KafkaAdminController.java). El panel permite crear, listar, cambiar retención y eliminar tópicos, además de consultar grupos. La API permite aumentar particiones, restablecer configuraciones y administrar offsets de grupos inactivos. |
| **Manejo de fallos (12%)** | [Configuración de reintentos y DLT](kafka-service/src/main/java/cl/duoc/kafka_service/config/KafkaInfraestructura.java). Los payloads inválidos van al tópico `.DLT` de su flujo. Los fallos transitorios admiten dos reintentos y los logs permiten seguir el evento. |
| **Parámetros de tópicos (12%)** | La pestaña **Tópicos** y Kafka UI muestran la retención, líderes, particiones y réplicas sincronizadas. El servicio `kafka-topics` en el Compose configura los tópicos de negocio. |
| **Grupos y reparto de carga (12%)** | En **Grupos**, revisa miembros, offsets y lag. Para comprobar el rebalanceo, inicia una segunda instancia de `kafka-service` con la misma configuración y `KAFKA_SERVICE_PORT=8084`. Observa cómo se reparten las particiones y vuelven al primer consumidor al detener la segunda instancia. |

### Demostración rápida

1. Crea una solicitud como **Cliente** y apruébala como **Empleado**. En **Kafka → Métricas**, comprueba que aparezcan los eventos de solicitud creada y cuenta habilitada.
2. En **Grupos**, comprueba que el offset confirmado alcance al final y que el lag sea cero en las particiones con eventos. En **RabbitMQ**, verifica que carguen colas, exchanges y bindings.
3. En **Kafka → Tópicos**, crea un tópico temporal como `bancocloud.ep5.prueba.frontend`. Cambia la retención de `86400000` a `172800000` ms y elimina ese tópico. El contador debe volver al valor inicial.

La prueba local registró **dos eventos de negocio** y el contador volvió a **cuatro tópicos** después de la eliminación. Las [pruebas de integración](kafka-service/src/test/java/cl/duoc/kafka_service/KafkaServiceIntegrationTest.java) se ejecutan con `.\mvnw.cmd test` desde `kafka-service`. Cubren consumo, deduplicación, reintentos, DLT, administración y roles. En Windows, el borrado se comprueba con el clúster de Docker porque se omite ese caso en Kafka embebido.
