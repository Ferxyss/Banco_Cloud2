import { isAxiosError } from "axios";
import api from "./api";

export type MetricaKafka = {
  flujo: string;
  tipoCuenta: string;
  eventos: number;
};

export type TopicoKafka = {
  nombre: string;
  configuraciones: Record<string, { valor: string; fuente: string }>;
  particiones: {
    numero: number;
    lider: number | null;
    replicas: number[];
    sincronizadas: number[];
  }[];
};

export type CrearTopico = {
  nombre: string;
  particiones: number;
  replicas: number;
  retencionMs: number;
};

export type GrupoKafka = {
  id: string;
  estado: string;
};

export type DetalleGrupoKafka = GrupoKafka & {
  miembros: { id: string; cliente: string }[];
  progreso: {
    topico: string;
    particion: number;
    offsetConfirmado: number | null;
    offsetFinal: number;
    lag: number | null;
  }[];
};

const base = "/api/admin/kafka";
const rutaTopico = (nombre: string) => base + "/topicos/" + encodeURIComponent(nombre);

async function consultar<T>(ruta: string, signal: AbortSignal): Promise<T> {
  const { data } = await api.get<T>(ruta, { signal, timeout: 20000 });
  return data;
}

export const kafkaAdmin = {
  metricas: (signal: AbortSignal) => consultar<MetricaKafka[]>(base + "/metricas", signal),
  topicos: (signal: AbortSignal) => consultar<string[]>(base + "/topicos", signal),
  topico: (nombre: string, signal: AbortSignal) =>
    consultar<TopicoKafka>(rutaTopico(nombre), signal),
  grupos: (signal: AbortSignal) => consultar<GrupoKafka[]>(base + "/grupos", signal),
  grupo: (id: string, signal: AbortSignal) =>
    consultar<DetalleGrupoKafka>(base + "/grupos/" + encodeURIComponent(id), signal),
  async crear(solicitud: CrearTopico): Promise<TopicoKafka> {
    const { data } = await api.post<TopicoKafka>(base + "/topicos", solicitud, { timeout: 20000 });
    return data;
  },
  async cambiarRetencion(nombre: string, retencionMs: number): Promise<TopicoKafka> {
    const { data } = await api.put<TopicoKafka>(
      rutaTopico(nombre) + "/retencion",
      { retencionMs },
      { timeout: 20000 },
    );
    return data;
  },
  async eliminar(nombre: string): Promise<void> {
    await api.delete(rutaTopico(nombre), { timeout: 20000 });
  },
};

export function mensajeKafka(error: unknown): string {
  if (isAxiosError<{ detail?: string }>(error)) {
    const estado = error.response?.status;
    if (estado === 401) return "Tu sesión expiró. Vuelve a iniciar sesión.";
    if (estado === 403) return "Tu usuario no tiene permiso para esta operación.";
    const detalle = error.response?.data?.detail;
    if (detalle) return detalle;
    if (estado === 404) return "El recurso ya no está disponible. Actualiza la lista.";
    if (estado === 503 || !estado) return "No pudimos conectar con Kafka. Intenta actualizar.";
  }
  return "No pudimos completar la operación. Intenta nuevamente.";
}
