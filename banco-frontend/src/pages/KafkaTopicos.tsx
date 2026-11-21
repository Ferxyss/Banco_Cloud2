import { useCallback, useState } from "react";
import type { FormEvent } from "react";
import { kafkaAdmin, mensajeKafka } from "../services/kafkaAdminService";
import type { TopicoKafka } from "../services/kafkaAdminService";
import useConsultaKafka from "./useConsultaKafka";

type Props = {
  topicos: string[] | null;
  cargando: boolean;
  error: string | null;
  revision: number;
  actualizar: () => void;
};

type Aviso = { texto: string; tipo: "success" | "error" };

const numeros = new Intl.NumberFormat("es-CL");

export default function KafkaTopicos({ topicos, cargando, error, revision, actualizar }: Props) {
  const [seleccion, setSeleccion] = useState("");
  const [aviso, setAviso] = useState<Aviso>();
  const [creando, setCreando] = useState(false);
  const nombre = topicos?.find((topico) => topico === seleccion) ?? topicos?.[0] ?? "";

  async function crear(evento: FormEvent<HTMLFormElement>) {
    evento.preventDefault();
    const formulario = evento.currentTarget;
    const campos = new FormData(formulario);
    setCreando(true);
    setAviso(undefined);
    try {
      const nuevo = await kafkaAdmin.crear({
        nombre: String(campos.get("nombre")).trim(),
        particiones: Number(campos.get("particiones")),
        replicas: Number(campos.get("replicas")),
        retencionMs: Number(campos.get("retencion")),
      });
      formulario.reset();
      setSeleccion(nuevo.nombre);
      setAviso({ texto: "Tópico creado: " + nuevo.nombre, tipo: "success" });
      actualizar();
    } catch (fallo) {
      setAviso({ texto: mensajeKafka(fallo), tipo: "error" });
    } finally {
      setCreando(false);
    }
  }

  function eliminado(topico: string) {
    setSeleccion((actual) => (actual === topico ? "" : actual));
    setAviso({ texto: "Eliminación aceptada: " + topico, tipo: "success" });
    actualizar();
  }

  return (
    <>
      {aviso && (
        <div className={"rabbit-admin-alert " + aviso.tipo}
          role={aviso.tipo === "error" ? "alert" : "status"}>
          {aviso.texto}
        </div>
      )}
      <div className="kafka-topicos">
        <article className="rabbit-admin-card">
          <h3>Tópicos registrados</h3>
          {cargando && <p role="status">Cargando tópicos…</p>}
          {error && <p className="rabbit-admin-alert error" role="alert">{error}</p>}
          {topicos?.length === 0 && <p>No hay tópicos registrados.</p>}
          {nombre && (
            <div className="kafka-topico-detalle">
              <label htmlFor="kafka-topico">Tópico</label>
              <select id="kafka-topico" value={nombre} onChange={(evento) => setSeleccion(evento.target.value)}>
                {topicos?.map((topico) => <option key={topico}>{topico}</option>)}
              </select>
              <DetalleTopico key={revision + "/" + nombre} nombre={nombre} eliminado={eliminado} />
            </div>
          )}
        </article>
        <article className="rabbit-admin-card">
          <h3>Crear tópico</h3>
          <form className="kafka-form" onSubmit={crear}>
            <label>
              Nombre
              <input name="nombre" required maxLength={249} pattern={"[A-Za-z0-9._\\x2d]+"}
                placeholder="bancocloud.ep5.prueba" disabled={creando} />
            </label>
            <div className="kafka-form-columns">
              <label>
                Particiones
                <input name="particiones" type="number" min={1} max={2147483647}
                  required defaultValue={3} disabled={creando} />
              </label>
              <label>
                Réplicas
                <input name="replicas" type="number" min={1} max={32767}
                  required defaultValue={3} disabled={creando} />
              </label>
            </div>
            <label>
              Retención (ms)
              <input name="retencion" type="number" min={-1} max={Number.MAX_SAFE_INTEGER}
                required defaultValue={86400000} disabled={creando} />
            </label>
            <p>86.400.000 ms equivalen a un día. Usa −1 para conservar los datos sin límite de tiempo.</p>
            <button type="submit" disabled={creando}>{creando ? "Creando…" : "Crear tópico"}</button>
          </form>
        </article>
      </div>
    </>
  );
}

function DetalleTopico({ nombre, eliminado }: { nombre: string; eliminado: (nombre: string) => void }) {
  const consultar = useCallback((signal: AbortSignal) => kafkaAdmin.topico(nombre, signal), [nombre]);
  const consulta = useConsultaKafka(consultar, 0);
  const [actualizado, setActualizado] = useState<TopicoKafka>();
  const [aviso, setAviso] = useState<Aviso>();
  const [operacion, setOperacion] = useState<"guardar" | "eliminar">();
  const [confirmando, setConfirmando] = useState(false);
  const detalle = actualizado ?? consulta.datos;

  async function cambiarRetencion(evento: FormEvent<HTMLFormElement>) {
    evento.preventDefault();
    const valor = Number(new FormData(evento.currentTarget).get("retencion"));
    setOperacion("guardar");
    setAviso(undefined);
    try {
      setActualizado(await kafkaAdmin.cambiarRetencion(nombre, valor));
      setAviso({ texto: "Retención actualizada.", tipo: "success" });
    } catch (fallo) {
      setAviso({ texto: mensajeKafka(fallo), tipo: "error" });
    } finally {
      setOperacion(undefined);
    }
  }

  async function borrar() {
    setOperacion("eliminar");
    setAviso(undefined);
    try {
      await kafkaAdmin.eliminar(nombre);
      eliminado(nombre);
    } catch (fallo) {
      setAviso({ texto: mensajeKafka(fallo), tipo: "error" });
      setOperacion(undefined);
    }
  }

  if (consulta.cargando) return <p role="status">Cargando particiones y configuración…</p>;
  if (!detalle) return <p className="rabbit-admin-alert error" role="alert">{consulta.error}</p>;

  const retencion = detalle.configuraciones["retention.ms"]?.valor;
  const retencionVisible = retencion === "-1" ? "Sin límite"
    : retencion == null ? "—" : numeros.format(Number(retencion)) + " ms";

  return (
    <>
      <dl className="kafka-configuracion">
        <div><dt>Retención</dt><dd>{retencionVisible}</dd></div>
        <div><dt>Política de limpieza</dt><dd>{detalle.configuraciones["cleanup.policy"]?.valor ?? "—"}</dd></div>
        <div><dt>Mínimo de réplicas sincronizadas</dt><dd>{detalle.configuraciones["min.insync.replicas"]?.valor ?? "—"}</dd></div>
      </dl>
      <div className="kafka-table-scroll">
        <table className="rabbit-admin-table">
          <caption className="kafka-sr-only">Particiones de {nombre}</caption>
          <thead><tr><th>Partición</th><th>Líder</th><th>Réplicas</th><th>Sincronizadas</th></tr></thead>
          <tbody>
            {detalle.particiones.map((particion) => (
              <tr key={particion.numero}>
                <td>{particion.numero}</td>
                <td>{particion.lider ?? "—"}</td>
                <td>{particion.replicas.join(", ") || "—"}</td>
                <td>{particion.sincronizadas.join(", ") || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {aviso && <div className={"rabbit-admin-alert " + aviso.tipo}
        role={aviso.tipo === "error" ? "alert" : "status"}>{aviso.texto}</div>}
      <form className="kafka-form kafka-retencion" onSubmit={cambiarRetencion}>
        <label>
          Nueva retención (ms)
          <input key={retencion} name="retencion" type="number" min={-1} max={Number.MAX_SAFE_INTEGER}
            defaultValue={retencion} disabled={Boolean(operacion)} required />
        </label>
        <button type="submit" disabled={Boolean(operacion)}>
          {operacion === "guardar" ? "Guardando…" : "Guardar retención"}
        </button>
      </form>
      {confirmando ? (
        <div className="kafka-confirmar">
          <p>¿Eliminar <strong>{nombre}</strong> y los eventos almacenados en el tópico?</p>
          <div className="kafka-acciones">
            <button type="button" disabled={Boolean(operacion)} onClick={() => setConfirmando(false)}>Cancelar</button>
            <button type="button" className="kafka-danger" disabled={Boolean(operacion)} onClick={() => void borrar()}>
              {operacion === "eliminar" ? "Eliminando…" : "Confirmar eliminación"}
            </button>
          </div>
        </div>
      ) : (
        <div className="kafka-acciones">
          <button type="button" className="kafka-danger" disabled={Boolean(operacion)}
            onClick={() => setConfirmando(true)}>Eliminar tópico</button>
        </div>
      )}
    </>
  );
}
