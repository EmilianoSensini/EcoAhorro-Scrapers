"""Map EAN codes to La Coope internal codes via the lacoopeencasa.coop API.

Lee los EANs del catalogo, consulta la API de forma concurrente con reintentos
y escribe los mapeos en ``mapeo_codigo_coope.csv`` de forma atomica.
"""
import asyncio
import csv
import os
import sys
from pathlib import Path

import aiohttp
from aiohttp import ClientResponseError


# Paths (relativos a la ubicacion del script)
SCRIPT_DIR = Path(__file__).resolve().parent
CATALOG_PATH = SCRIPT_DIR / ".." / ".." / "data" / "catalogo_productos.csv"
OUTPUT_TMP = SCRIPT_DIR / "mapeo_codigo_coope.csv.tmp"
OUTPUT_FINAL = SCRIPT_DIR / "mapeo_codigo_coope.csv"

# API
API_BASE = "https://api.lacoopeencasa.coop/api/buscar"
SMOKE_EAN = "7794000006188"

# Concurrencia / reintentos
SEMAPHORE_LIMIT = 15
MAX_RETRIES = 3
RATE_LIMIT_EXTRA_SLEEP = 5.0

# HTTP
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
TIMEOUT = aiohttp.ClientTimeout(total=10)

# Progreso
PROGRESS_EVERY = 100

# Estado compartido (asyncio es single-threaded: no requiere lock)
stats = {"total": 0, "encontrados": 0, "no_encontrados": 0, "errores": 0}
progress_counter = 0

semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
csv_lock = asyncio.Lock()


def load_eans() -> list[str]:
    """Lee el catalogo y devuelve la lista de EANs (columna ``id``)."""
    eans: list[str] = []
    with open(CATALOG_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ean = (row.get("id") or "").strip()
            if ean:
                eans.append(ean)
    return eans


async def smoke_test(session: aiohttp.ClientSession) -> None:
    """Verifica que la API responde y la estructura del JSON es la esperada."""
    url = f"{API_BASE}?q={SMOKE_EAN}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"SMOKE TEST FALLIDO: status code {resp.status}")
                sys.exit(1)
            data = await resp.json(content_type=None)
    except Exception as e:
        print(f"SMOKE TEST FALLIDO: {type(e).__name__}: {e}")
        sys.exit(1)

    try:
        productos = data["datos"]["producto"]
        if not productos:
            print("SMOKE TEST FALLIDO: datos.producto vacio")
            sys.exit(1)
        if not productos[0].get("cod_interno"):
            print("SMOKE TEST FALLIDO: primer producto sin cod_interno")
            sys.exit(1)
    except (KeyError, TypeError, IndexError) as e:
        print(f"SMOKE TEST FALLIDO: estructura inesperada - {e}")
        sys.exit(1)

    print("Smoke test OK - API respondio correctamente")


async def fetch_cod_interno(
    session: aiohttp.ClientSession, ean: str
) -> str | None:
    """Devuelve el ``cod_interno`` del primer producto para el EAN dado.

    - ``None`` si la API responde OK pero ``datos.producto`` esta vacio.
    - Lanza excepcion si el status HTTP no es reintentable o si se agotan
      los reintentos tras errores de red / 5xx / 429.
    """
    url = f"{API_BASE}?q={ean}"
    last_error: str = ""

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url) as resp:
                if resp.status == 429:
                    last_error = f"HTTP 429 (intento {attempt + 1})"
                    await asyncio.sleep(RATE_LIMIT_EXTRA_SLEEP + (2 ** attempt))
                    continue
                if 500 <= resp.status < 600:
                    last_error = f"HTTP {resp.status} (intento {attempt + 1})"
                    await asyncio.sleep(2 ** attempt)
                    continue
                if resp.status != 200:
                    raise ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                    )
                data = await resp.json(content_type=None)

        except (
            aiohttp.ClientConnectorError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientOSError,
            aiohttp.ClientPayloadError,
            aiohttp.ServerTimeoutError,
            asyncio.TimeoutError,
        ) as e:
            last_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2 ** attempt)
            continue

        # Exito HTTP: parsear el payload
        productos = data.get("datos", {}).get("producto", [])
        if not productos:
            return None
        cod_interno = productos[0].get("cod_interno")
        if not cod_interno:
            raise RuntimeError(f"cod_interno vacio para EAN {ean}")
        return str(cod_interno)

    raise RuntimeError(
        f"Max reintentos agotados para EAN {ean}: {last_error}"
    )


async def process_ean(
    ean: str,
    session: aiohttp.ClientSession,
    writer: csv.DictWriter,
    f_out,
) -> None:
    """Procesa un EAN: consulta la API y persiste el mapeo si existe."""
    global progress_counter
    async with semaphore:
        try:
            cod_interno = await fetch_cod_interno(session, ean)
        except Exception as e:
            print(f"Error en EAN {ean}: {e}")
            stats["errores"] += 1
        else:
            if cod_interno is None:
                stats["no_encontrados"] += 1
            else:
                stats["encontrados"] += 1
                async with csv_lock:
                    writer.writerow(
                        {"ean": ean, "codigo_interno_coope": cod_interno}
                    )
                    f_out.flush()

        progress_counter += 1
        if progress_counter % PROGRESS_EVERY == 0:
            print(
                f"Progreso: {progress_counter}/{stats['total']} EANs procesados"
            )


async def main() -> None:
    eans = load_eans()
    stats["total"] = len(eans)

    if not eans:
        print("No se encontraron EANs en el catalogo")
        sys.exit(1)

    async with aiohttp.ClientSession(timeout=TIMEOUT, headers=HEADERS) as session:
        await smoke_test(session)

        f_out = open(OUTPUT_TMP, "w", encoding="utf-8", newline="")
        writer = csv.DictWriter(
            f_out, fieldnames=["ean", "codigo_interno_coope"]
        )
        writer.writeheader()
        f_out.flush()

        try:
            tasks = [
                asyncio.create_task(process_ean(ean, session, writer, f_out))
                for ean in eans
            ]
            await asyncio.gather(*tasks)
        finally:
            f_out.close()

    # Reemplazo atomico: tmp -> final
    os.replace(OUTPUT_TMP, OUTPUT_FINAL)

    print("=== RESUMEN FINAL ===")
    print(f"Total EANs procesados: {stats['total']}")
    print(f"Encontrados: {stats['encontrados']}")
    print(f"No encontrados: {stats['no_encontrados']}")
    print(f"Errores de red/API: {stats['errores']}")
    print(f"CSV generado: {OUTPUT_FINAL.name}")
    print("=====================")


if __name__ == "__main__":
    asyncio.run(main())
