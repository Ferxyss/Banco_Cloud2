import { useEffect, useState } from "react";
import { mensajeKafka } from "../services/kafkaAdminService";

type Resultado<T> = {
  datos: T | null;
  error: string | null;
};

export default function useConsultaKafka<T>(
  consultar: (signal: AbortSignal) => Promise<T>,
  revision: number,
): Resultado<T> & { cargando: boolean } {
  const [estado, setEstado] = useState<{
    consultar: typeof consultar;
    revision: number;
    resultado: Resultado<T>;
  }>();

  useEffect(() => {
    const control = new AbortController();

    async function cargar() {
      let resultado: Resultado<T>;
      try {
        resultado = { datos: await consultar(control.signal), error: null };
      } catch (error) {
        resultado = { datos: null, error: mensajeKafka(error) };
      }

      if (!control.signal.aborted) {
        setEstado({ consultar, revision, resultado });
      }
    }

    void cargar();
    return () => control.abort();
  }, [consultar, revision]);

  if (estado?.consultar !== consultar || estado.revision !== revision) {
    return { datos: null, error: null, cargando: true };
  }
  return { ...estado.resultado, cargando: false };
}
