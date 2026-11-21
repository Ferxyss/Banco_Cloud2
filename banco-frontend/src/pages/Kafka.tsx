import { useCallback, useState } from "react";
import Sidebar from "../components/Sidebar";
import { kafkaAdmin } from "../services/kafkaAdminService";
import type { GrupoKafka, MetricaKafka } from "../services/kafkaAdminService";
import KafkaTopicos from "./KafkaTopicos";
import useConsultaKafka from "./useConsultaKafka";
import "./RabbitMQ.css";
import "./Kafka.css";

const numeros = new Intl.NumberFormat("es-CL");
const etiquetasFlujo: Record<string, string> = {
  SOLICITUD_CREADA: "Solicitudes creadas",
  CUENTA_HABILITADA: "Cuentas habilitadas",
};
const secciones = [
  { id: "metricas", nombre: "Métricas" },
  { id: "topicos", nombre: "Tópicos" },
  { id: "grupos", nombre: "Grupos" },
] as const;

export default function Kafka() {
  const [seccion, setSeccion] = useState<(typeof secciones)[number]["id"]>("metricas");
  const [revision, setRevision] = useState(0);
  const metricas = useConsultaKafka(kafkaAdmin.metricas, revision);
  const topicos = useConsultaKafka(kafkaAdmin.topicos, revision);
  const grupos = useConsultaKafka(kafkaAdmin.grupos, revision);
  const cargando = metricas.cargando || topicos.cargando || grupos.cargando;
  const total = metricas.datos?.reduce((suma, fila) => suma + fila.eventos, 0);

  function actualizar() {
    setRevision((actual) => actual + 1);
  }

  return (
    <div className="rabbit-page kafka-page">
      <Sidebar />
      <main className="rabbit-main">
        <header className="rabbit-header">
          <div>
            <div className="rabbit-breadcrumb">BANCO CLOUD / INFRAESTRUCTURA</div>
            <h1>Kafka</h1>
            <p>Monitoreo y administración de eventos del banco.</p>
          </div>
        </header>

        <section className="rabbit-hero">
          <div className="rabbit-hero-content">
            <span className="rabbit-eyebrow">PROCESAMIENTO DE EVENTOS</span>
            <h2>Solicitudes y cuentas en Kafka</h2>
            <p>Consulta los eventos procesados y comprueba el avance de los grupos de consumidores.</p>
          </div>
          <div className="rabbit-hero-icon" aria-hidden="true"><span>K</span></div>
        </section>

        <section className="rabbit-admin">
          <div className="rabbit-admin-heading">
            <div>
              <h2>Administración de Kafka</h2>
              <p>Consulta métricas, administra tópicos y revisa los offsets por partición.</p>
            </div>
            <button type="button" onClick={actualizar} disabled={cargando}>
              {cargando ? "Actualizando…" : "↻ Actualizar"}
            </button>
          </div>

          <div className="rabbit-admin-grid">
            <article className="rabbit-admin-card">
              <span>EVENTOS PROCESADOS</span>
              <h3>{total == null ? "—" : numeros.format(total)}</h3>
              <p>Solicitudes y cuentas registradas por el consumidor</p>
            </article>
            <article className="rabbit-admin-card">
              <span>TÓPICOS</span>
              <h3>{topicos.datos?.length ?? "—"}</h3>
              <p>Tópicos registrados en el clúster</p>
            </article>
            <article className="rabbit-admin-card">
              <span>GRUPOS DE CONSUMIDORES</span>
              <h3>{grupos.datos?.length ?? "—"}</h3>
              <p>Grupos registrados en Kafka</p>
            </article>
          </div>

          <div className="rabbit-admin-tabs" role="group" aria-label="Secciones de Kafka">
            {secciones.map(({ id, nombre }) => (
              <button key={id} type="button" aria-pressed={seccion === id}
                className={seccion === id ? "active" : ""} onClick={() => setSeccion(id)}>
                {nombre}
              </button>
            ))}
          </div>

          {seccion === "metricas" && (
            <article className="rabbit-admin-card">
              <h3>Eventos por tipo de cuenta</h3>
              {metricas.cargando && <p role="status">Cargando métricas…</p>}
              {metricas.error && <p className="rabbit-admin-alert error" role="alert">{metricas.error}</p>}
              {metricas.datos?.length === 0 && <p>Todavía no hay eventos procesados.</p>}
              {metricas.datos && metricas.datos.length > 0 && <TablaMetricas metricas={metricas.datos} />}
            </article>
          )}

          {seccion === "topicos" && (
            <KafkaTopicos topicos={topicos.datos} cargando={topicos.cargando} error={topicos.error}
              revision={revision} actualizar={actualizar} />
          )}

          {seccion === "grupos" && (
            <article className="rabbit-admin-card">
              <h3>Grupos de consumidores</h3>
              {grupos.cargando && <p role="status">Cargando grupos…</p>}
              {grupos.error && <p className="rabbit-admin-alert error" role="alert">{grupos.error}</p>}
              {grupos.datos?.length === 0 && <p>No hay grupos de consumidores registrados.</p>}
              {grupos.datos && grupos.datos.length > 0 && <Grupos grupos={grupos.datos} revision={revision} />}
            </article>
          )}
        </section>
      </main>
    </div>
  );
}

function TablaMetricas({ metricas }: { metricas: MetricaKafka[] }) {
  return (
    <div className="kafka-table-scroll">
      <table className="rabbit-admin-table">
        <caption className="kafka-sr-only">Eventos procesados por flujo y tipo de cuenta</caption>
        <thead><tr><th>Flujo</th><th>Tipo de cuenta</th><th>Eventos</th></tr></thead>
        <tbody>
          {metricas.map((fila) => (
            <tr key={fila.flujo + "/" + fila.tipoCuenta}>
              <td>{etiquetasFlujo[fila.flujo] ?? fila.flujo}</td>
              <td>{fila.tipoCuenta}</td>
              <td>{numeros.format(fila.eventos)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Grupos({ grupos, revision }: { grupos: GrupoKafka[]; revision: number }) {
  const [seleccion, setSeleccion] = useState("");
  const grupo = grupos.find((item) => item.id === seleccion) ?? grupos[0];

  return (
    <div className="kafka-grupo">
      <label htmlFor="kafka-grupo">Grupo</label>
      <select id="kafka-grupo" value={grupo.id} onChange={(evento) => setSeleccion(evento.target.value)}>
        {grupos.map(({ id }) => <option key={id}>{id}</option>)}
      </select>
      <DetalleGrupo id={grupo.id} revision={revision} />
    </div>
  );
}

function DetalleGrupo({ id, revision }: { id: string; revision: number }) {
  const consultar = useCallback((signal: AbortSignal) => kafkaAdmin.grupo(id, signal), [id]);
  const { datos, cargando, error } = useConsultaKafka(consultar, revision);

  if (cargando) return <p role="status">Consultando consumidores y offsets…</p>;
  if (!datos) return <p className="rabbit-admin-alert error" role="alert">{error}</p>;

  return (
    <>
      <dl className="kafka-configuracion">
        <div>
          <dt>Estado</dt>
          <dd>
            <span className={"rabbit-admin-status" + (datos.estado.toLowerCase() === "stable" ? "" : " kafka-estado")}>
              {datos.estado}
            </span>
          </dd>
        </div>
        <div><dt>Miembros activos</dt><dd>{numeros.format(datos.miembros.length)}</dd></div>
      </dl>
      {datos.progreso.length === 0 ? (
        <p>El grupo todavía no tiene particiones con progreso registrado.</p>
      ) : (
        <div className="kafka-table-scroll">
          <table className="rabbit-admin-table">
            <caption className="kafka-sr-only">Progreso del grupo {id} por partición</caption>
            <thead>
              <tr><th>Tópico</th><th>Partición</th><th>Offset confirmado</th><th>Offset final</th><th>Pendientes (lag)</th></tr>
            </thead>
            <tbody>
              {datos.progreso.map((fila) => (
                <tr key={fila.topico + "/" + fila.particion}>
                  <td>{fila.topico}</td>
                  <td>{fila.particion}</td>
                  <td>{fila.offsetConfirmado == null ? "—" : numeros.format(fila.offsetConfirmado)}</td>
                  <td>{numeros.format(fila.offsetFinal)}</td>
                  <td>{fila.lag == null ? "—" : numeros.format(fila.lag)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
