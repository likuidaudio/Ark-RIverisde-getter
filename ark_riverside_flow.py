#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║   ARK MEDIA — Riverside → Renombrado → Google Drive  ║
╚══════════════════════════════════════════════════════╝

Uso:
  python ark_riverside_flow.py           → Ventana de escritorio
  python ark_riverside_flow.py --setup   → Configurar studios (terminal)

Convención de nombres:
  Una toma:  CMB_469_Dan.wav
  Varias:    CMB_469_Dan_Take02.wav

Configuración: config.json  (API key + studio IDs por show)
"""

__version__ = "1.0.0"   # ✅ versión estable — 2026-03-09

import os, re, sys, json, shutil, datetime, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue as q_module
from pathlib import Path

# Forzar UTF-8 en Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── Plataforma ─────────────────────────────────────────────────────────────────
IS_MAC   = sys.platform == "darwin"
IS_WIN   = sys.platform == "win32"
FONT_UI  = "Helvetica Neue" if IS_MAC else "Segoe UI"
FONT_MONO = "Monaco"        if IS_MAC else "Consolas"

# ── Directorio base (funciona tanto en .py como en .exe compilado) ─────────────
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

# ── Logger (modo terminal) ─────────────────────────────────────────────────────

LOG_PATH = BASE_DIR / "ark_log.txt"

class Tee:
    """Duplica stdout → pantalla + archivo."""
    def __init__(self, file):
        self._file   = file
        self._stdout = sys.stdout
    def write(self, data):
        self._stdout.write(data)
        self._file.write(data)
    def flush(self):
        self._stdout.flush()
        self._file.flush()
    def reconfigure(self, **kwargs):
        self._stdout.reconfigure(**kwargs)

def _init_terminal_log():
    f = open(LOG_PATH, "a", encoding="utf-8")
    f.write(f"\n{'='*56}\n{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*56}\n")
    sys.stdout = Tee(f)
    return f

# ── Dependencias ───────────────────────────────────────────────────────────────

def verificar_dependencias():
    faltantes = []
    try:   import requests
    except ImportError: faltantes.append("requests")
    if faltantes:
        print("\n  ✗ Faltan dependencias. Corré en la terminal:")
        for dep in faltantes: print(f"    pip install {dep}")
        sys.exit(1)

verificar_dependencias()
import requests

# ── Constantes ─────────────────────────────────────────────────────────────────

RIVERSIDE_BASE = "https://platform.riverside.fm"

# Valores por defecto según plataforma — se sobreescriben con config.json al iniciar
DRIVE_BASE = (
    Path.home() / "Library" / "CloudStorage"
    if IS_MAC else
    Path(r"G:\Unidades compartidas\ARK MEDIA")
)

SHOWS = {
    "CMB":  "Call Me Back",
    "ICMB": "Inside Call Me Back",
    "WYN":  "What's Your Number",
}

# Nombres de hosts conocidos por show — usados como pistas adicionales
# en el matching de recordings. Se pueden extender en config.json.
SHOW_HOSTS = {
    "CMB":  ["Dan"],
    "ICMB": ["Dan", "Deborah"],
    "WYN":  ["Yonatan", "Yael"],
}

DRIVE_FOLDERS = {
    "CMB":  "Production_Call me Back",
    "ICMB": "Production_Inside Call me Back",
    "WYN":  "Production_What's Your Number",
}

# Carpeta local (DaVinci Resolve) — valores por defecto según plataforma
LOCAL_BASE = (
    Path.home() / "Movies" / "DaVinci Resolve"
    if IS_MAC else
    Path(r"C:\DaVinci Projects")
)

LOCAL_SHOW_FOLDERS = {
    "CMB":  "CMB",
    "ICMB": "ICMB",
    "WYN":  "WYN",
}


def aplicar_config_carpetas(config: dict):
    """Sobreescribe las rutas de Drive y locales con los valores guardados en config.json."""
    global DRIVE_BASE, DRIVE_FOLDERS, LOCAL_BASE, LOCAL_SHOW_FOLDERS, SHOW_HOSTS
    if config.get("drive_base"):
        DRIVE_BASE = Path(config["drive_base"])
    if config.get("drive_folders"):
        DRIVE_FOLDERS.update(config["drive_folders"])
    if config.get("local_base"):
        LOCAL_BASE = Path(config["local_base"])
    if config.get("local_show_folders"):
        LOCAL_SHOW_FOLDERS.update(config["local_show_folders"])
    if config.get("show_hosts"):
        SHOW_HOSTS.update(config["show_hosts"])

SEPARATOR = "─" * 56


# ══════════════════════════════════════════════════════
#   MÓDULO 0 — config.json
# ══════════════════════════════════════════════════════

CONFIG_PATH = BASE_DIR / "config.json"

def cargar_config() -> dict:
    if not CONFIG_PATH.exists():
        config = {"riverside_api_key": "PEGAR_API_KEY_AQUI"}
        with open(CONFIG_PATH, "w") as f: json.dump(config, f, indent=2)
        print("\n  ✗ Creé config.json. Abrilo y pegá tu API key de Riverside.")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    api_key = config.get("riverside_api_key", "")
    if not api_key or api_key == "PEGAR_API_KEY_AQUI":
        print("\n  ✗ Completá la API key de Riverside en config.json")
        sys.exit(1)
    return config

def guardar_config(config: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}"}


# ══════════════════════════════════════════════════════
#   MÓDULO 1 — Configuración de Studios (--setup)
# ══════════════════════════════════════════════════════

def _buscar_studio_por_recording_id(api_key, recording_id):
    try:
        r = requests.get(f"{RIVERSIDE_BASE}/api/v2/recordings", headers=_headers(api_key), timeout=30)
        if r.status_code != 200: return None, None
        data = r.json()
        recs = data if isinstance(data, list) else data.get("recordings") or data.get("data") or []
        for rec in recs:
            rid = str(rec.get("id") or rec.get("recording_id") or "")
            if recording_id.lower() in rid.lower() or rid.lower() in recording_id.lower():
                return str(rec.get("studio_id") or ""), rec.get("studio_name") or ""
    except Exception: pass
    return None, None

def _buscar_studio_por_endpoint(api_key):
    try:
        r = requests.get(f"{RIVERSIDE_BASE}/api/v2/studios", headers=_headers(api_key), timeout=30)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else data.get("studios") or data.get("data") or []
    except Exception: pass
    return []

def _extraer_studios_de_recordings(api_key):
    vistos = {}
    for page in range(1, 21):
        try:
            r = requests.get(f"{RIVERSIDE_BASE}/api/v2/recordings",
                             headers=_headers(api_key), params={"page": page}, timeout=30)
            if r.status_code != 200: break
            data = r.json()
            recs = data if isinstance(data, list) else data.get("recordings") or data.get("data") or []
        except Exception: break
        if not recs: break
        for rec in recs:
            sid  = str(rec.get("studio_id") or "")
            snom = rec.get("studio_name") or ""
            if sid and sid not in vistos:
                vistos[sid] = {"id": sid, "name": snom}
    return list(vistos.values())

def _parsear_url_riverside(url):
    slug = project_id = None
    m = re.search(r"/studios/([^/?#]+)", url)
    if m: slug = m.group(1)
    m2 = re.search(r"/projects/([^/?#]+)", url)
    if m2: project_id = m2.group(1)
    return {"slug": slug, "project_id": project_id}

def _buscar_studio_por_proyecto(api_key, project_id):
    try:
        r = requests.get(f"{RIVERSIDE_BASE}/api/v2/recordings",
                         headers=_headers(api_key), params={"projectId": project_id}, timeout=30)
        if r.status_code == 200:
            data = r.json()
            recs = data if isinstance(data, list) else data.get("recordings") or data.get("data") or []
            if recs:
                rec = recs[0]
                return str(rec.get("studio_id") or ""), rec.get("studio_name") or ""
    except Exception: pass
    return None, None

def _probar_studio_id(api_key, studio_id):
    try:
        r = requests.get(f"{RIVERSIDE_BASE}/api/v2/recordings",
                         headers=_headers(api_key), params={"studioId": studio_id}, timeout=20)
        if r.status_code == 200:
            data = r.json()
            recs = data if isinstance(data, list) else data.get("recordings") or data.get("data") or []
            return len(recs) > 0
    except Exception: pass
    return False

def configurar_studios(config: dict, forzar: bool = False) -> dict:
    api_key    = config["riverside_api_key"]
    studio_ids = config.get("studio_ids", {}) if not forzar else {}

    print("CONFIGURACION DE STUDIOS")
    print(SEPARATOR)
    print()
    print("  Para cada show, abrí cualquier proyecto en Riverside y pegá la URL.")
    print()

    for codigo, nombre_show in SHOWS.items():
        actual = studio_ids.get(codigo)
        if actual and not forzar:
            print(f"  {codigo} ({nombre_show}): configurado [studio_id={actual}]")
            mantener = input(f"    Enter para mantener, o pegá URL/ID nuevo: ").strip()
            if not mantener:
                print(f"    ✓ Manteniendo\n")
                continue
            entrada = mantener
        else:
            print(f"  {codigo} ({nombre_show}):")
            entrada = input(f"    URL del show o ID de grabacion (Enter para lista): ").strip()
        print()

        if entrada:
            if "riverside.com" in entrada or "/studios/" in entrada:
                partes     = _parsear_url_riverside(entrada)
                slug       = partes["slug"]
                project_id = partes["project_id"]
                if project_id:
                    print(f"    → Project ID detectado: '{project_id}'. Consultando studio...")
                    sid, snom = _buscar_studio_por_proyecto(api_key, project_id)
                    if sid:
                        studio_ids[codigo] = sid
                        print(f"    ✓ {codigo} → studio '{snom}' (id: {sid})\n")
                        continue
                    else:
                        print(f"    ✗ No se pudo obtener el studio de ese proyecto.\n")
                if slug:
                    print(f"    → Slug detectado: '{slug}'. Probando como studioId...")
                    if _probar_studio_id(api_key, slug):
                        studio_ids[codigo] = slug
                        print(f"    ✓ {codigo} → studio '{slug}' funciona\n")
                        continue
                    else:
                        print(f"    ✗ El slug no funcionó. Pasando a la lista...\n")
            else:
                print(f"    Buscando en la API el studio de esa grabacion...")
                sid, snom = _buscar_studio_por_recording_id(api_key, entrada)
                if sid:
                    studio_ids[codigo] = sid
                    print(f"    ✓ {codigo} → studio '{snom}' (id: {sid})\n")
                    continue
                else:
                    print(f"    ✗ No encontré esa grabacion. Pasando a la lista...\n")

        studios = _buscar_studio_por_endpoint(api_key) or _extraer_studios_de_recordings(api_key)
        if studios:
            print(f"    Studios disponibles:\n")
            for idx, s in enumerate(studios, 1):
                snom = s.get("name") or s.get("title") or "Sin nombre"
                sid  = s.get("id")   or s.get("studio_id") or "?"
                print(f"      [{idx}] {snom}  (id: {sid})")
            print()
            while True:
                sel = input(f"    {codigo} — numero [1-{len(studios)}] o Enter para saltar: ").strip()
                if sel == "":
                    print(); break
                if sel.isdigit() and 1 <= int(sel) <= len(studios):
                    s = studios[int(sel) - 1]
                    studio_ids[codigo] = str(s.get("id") or s.get("studio_id"))
                    print(f"    ✓ {codigo} → {s.get('name')} (id: {studio_ids[codigo]})\n")
                    break
                print(f"    ✗ Ingresa un numero entre 1 y {len(studios)}, o Enter para saltar.")
        else:
            print(f"    ✗ No se encontraron studios. Salteando {codigo}.\n")

    config["studio_ids"] = studio_ids
    guardar_config(config)
    print("  ✓ Configuración guardada en config.json\n")
    return config


# ══════════════════════════════════════════════════════
#   MÓDULO 2 — Descarga desde Riverside
# ══════════════════════════════════════════════════════

_EXPORT_PATTERNS = re.compile(
    r'\bcop[yi]\b'
    r'|\bv\d+\b'
    r'|\bclip\b'
    r'|\btimeline\b'
    r'|\bexport\b'
    r'|\bSM\b'
    r'|\bedit\b'
    r'|\bfinal\b'
    r'|\bteaser\b'
    r'|\btrailer\b'
    r'|\bexcerpt\b'
    r'|\bpromo\b'
    r'|\bshort\b'
    r'|\bhighlight\b'
    r'|\bvideo\b',
    re.IGNORECASE,
)

def _es_export(rec: dict) -> bool:
    nombre = rec.get("name") or rec.get("title") or ""
    return bool(_EXPORT_PATTERNS.search(nombre))


def _pagina_tiene_resultados(api_key: str, base_params: dict, page: int) -> bool:
    """Devuelve True si la página tiene al menos una grabación."""
    try:
        r = requests.get(f"{RIVERSIDE_BASE}/api/v2/recordings",
                         headers=_headers(api_key),
                         params={**base_params, "page": page}, timeout=20)
        if r.status_code != 200: return False
        data = r.json()
        recs = data if isinstance(data, list) else data.get("recordings") or data.get("data") or []
        return len(recs) > 0
    except Exception:
        return False


def _encontrar_ultima_pagina(api_key: str, base_params: dict) -> int:
    """
    Búsqueda binaria para encontrar la última página con resultados.
    Usa ~6-7 llamadas a la API en vez de paginar todo desde el inicio.
    """
    lo, hi = 1, 150  # asumimos máx 3000 grabaciones (150 páginas)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _pagina_tiene_resultados(api_key, base_params, mid):
            lo = mid
        else:
            hi = mid - 1
    return lo


def _fetch_page(api_key: str, base_params: dict, page: int):
    """Descarga una sola página; devuelve (page, lista_de_recs) o (page, None) si error."""
    try:
        r = requests.get(f"{RIVERSIDE_BASE}/api/v2/recordings",
                         headers=_headers(api_key),
                         params={**base_params, "page": page}, timeout=30)
        if r.status_code == 401:
            return page, "AUTH_ERROR"
        if r.status_code != 200:
            return page, None
        data = r.json()
        recs = data if isinstance(data, list) else data.get("recordings") or data.get("data") or []
        return page, recs if recs else None
    except Exception:
        return page, None


def _obtener_recordings(api_key: str, studio_id=None, project_id=None) -> list:
    """
    Fetchea las grabaciones MÁS RECIENTES primero.
    Búsqueda binaria para encontrar la última página, luego descarga
    las últimas 40 páginas (≈800 grabaciones) en paralelo.
    """
    if project_id:     base_params = {"projectId": project_id}
    elif studio_id:    base_params = {"studioId":  studio_id}
    else:              base_params = {}

    PAGINAS_A_TRAER = 40   # 40 × 20 = 800 grabaciones más recientes
    MAX_WORKERS     = 10   # 10 conexiones simultáneas

    print(f"\r  → Localizando grabaciones recientes...", end="", flush=True)
    ultima  = _encontrar_ultima_pagina(api_key, base_params)
    primera = max(1, ultima - PAGINAS_A_TRAER + 1)
    paginas = list(range(primera, ultima + 1))

    resultados = {}   # page → recs
    completadas = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futuros = {pool.submit(_fetch_page, api_key, base_params, p): p for p in paginas}
        for fut in as_completed(futuros):
            page, recs = fut.result()
            completadas += 1
            print(f"\r  → Cargando... {completadas}/{len(paginas)} páginas", end="", flush=True)
            if recs == "AUTH_ERROR":
                print("\n  ✗ API key inválida. Verificá config.json.")
                return []
            if recs:
                resultados[page] = recs

    # Reconstituir en orden descendente (más reciente primero)
    todas = []
    for page in sorted(resultados.keys(), reverse=True):
        todas.extend(resultados[page])

    print(f"\r  → {len(todas)} grabaciones cargadas (páginas {primera}–{ultima}).          ")
    return todas


def _sufijo_toma(recording_nombre: str, idx: int) -> str:
    m = re.search(r'take\s*(\d+)', recording_nombre, re.IGNORECASE)
    if m: return f"_Take{int(m.group(1)):02d}"
    return f"_{idx}"

def _nombre_participante_desde_id(track_id: str, fallback: str) -> str:
    if not track_id: return fallback
    parte = track_id.split("-")[0]
    return parte.capitalize() if parte else fallback


def _descargar_tracks(recording: dict, carpeta_destino: Path, api_key: str,
                      sufijo: str = "", progress_cb=None,
                      bajar_audio: bool = True, bajar_video: bool = True) -> list:
    """
    Descarga audio y/o video de cada track de un recording.
    progress_cb(pct, nombre_archivo) se llama opcionalmente con el progreso.
    """
    nombre_gr = recording.get("name") or recording.get("title") or "Sin nombre"

    EXCLUIR_TIPOS   = {"composite", "all", "screenshare", "screen_share", "all_participants"}
    EXCLUIR_NOMBRES = {"all participants", "composite", "screen share"}

    tracks = recording.get("tracks", [])
    participant_tracks = [
        t for t in tracks
        if  t.get("type", "").lower() not in EXCLUIR_TIPOS
        and t.get("display_name", t.get("name", "")).lower() not in EXCLUIR_NOMBRES
    ]

    if not participant_tracks:
        print(f"  ✗ [{nombre_gr}] No se encontraron tracks de participantes.")
        for t in tracks:
            print(f"    type={t.get('type')}  status={t.get('status')}  files={bool(t.get('files'))}")
        return []

    print(f"  Tracks ({len(participant_tracks)}):")
    for t in participant_tracks:
        nombre_t    = t.get("track_name") or t.get("name") or "?"
        tiene_files = bool(t.get("files"))
        print(f"    {nombre_t}  |  type={t.get('type')}  files={'si' if tiene_files else 'NO'}")
    print()

    sin_archivos = [t for t in participant_tracks if not t.get("files")]
    if sin_archivos:
        nombres_s = [t.get("track_name") or t.get("name") or "?" for t in sin_archivos]
        print(f"  ⚠  {len(sin_archivos)} pista(s) sin archivos (procesando en Riverside): {nombres_s}")
        print(f"     Esperá unos minutos y volvé a intentar.")
        participant_tracks = [t for t in participant_tracks if t.get("files")]
        if not participant_tracks: return []

    CLAVES_AUDIO = ["raw_audio", "lossless_audio", "audio", "compressed_audio"]
    CLAVES_VIDEO = ["raw_video", "video", "compressed_video"]

    archivos_descargados = []

    for i, track in enumerate(participant_tracks, 1):
        track_id = str(track.get("id") or track.get("track_id") or "")
        nombre   = (
            track.get("track_name") or track.get("display_name") or
            track.get("participant_name") or track.get("user_name") or
            track.get("name") or
            _nombre_participante_desde_id(track_id, f"Participante_{i}")
        )
        print(f"  [{i}/{len(participant_tracks)}]  {nombre}")

        files    = track.get("files") or track.get("assets") or {}
        descargas = []  # [(url, ext), ...]

        if isinstance(files, dict):
            if bajar_audio:
                for clave in CLAVES_AUDIO:
                    if files.get(clave):
                        ext = ".wav" if clave in ("raw_audio", "lossless_audio") else ".mp3"
                        descargas.append((files[clave], ext)); break
            if bajar_video:
                for clave in CLAVES_VIDEO:
                    if files.get(clave):
                        descargas.append((files[clave], ".mp4")); break
        elif isinstance(files, list):
            tiene_audio = tiene_video = False
            for f in files:
                ftype = f.get("type", "").lower()
                url   = f.get("url") or f.get("download_url")
                if not url: continue
                if "audio" in ftype and not tiene_audio and bajar_audio:
                    ext = ".wav" if ("raw" in ftype or "lossless" in ftype) else ".mp3"
                    descargas.append((url, ext)); tiene_audio = True
                elif "video" in ftype and not tiene_video and bajar_video:
                    descargas.append((url, ".mp4")); tiene_video = True

        if not descargas and bajar_audio:
            dl_url = track.get("download_url") or track.get("url")
            if dl_url: descargas.append((dl_url, ".wav"))

        if not descargas:
            print(f"       ✗ No se encontro URL de descarga.")
            print(f"         type:  {track.get('type')}")
            print(f"         files: {json.dumps(files, indent=10)[:400] if files else '(vacío)'}")
            continue

        for dl_url, ext in descargas:
            nombre_archivo = f"{nombre}{sufijo}{ext}".replace(" ", "_")
            ruta_final     = carpeta_destino / nombre_archivo
            try:
                with requests.get(dl_url, headers=_headers(api_key), stream=True, timeout=120) as r:
                    r.raise_for_status()
                    total      = int(r.headers.get("content-length", 0))
                    descargado = 0
                    with open(ruta_final, "wb") as out:
                        for chunk in r.iter_content(chunk_size=65536):
                            out.write(chunk)
                            descargado += len(chunk)
                            if total and progress_cb:
                                progress_cb(int(descargado / total * 100), nombre_archivo)
                            elif total:
                                pct = int(descargado / total * 100)
                                print(f"\r       Descargando... {pct}%", end="", flush=True)
                print(f"\r       ✓ {nombre_archivo}          ")
                archivos_descargados.append({
                    "ruta":                   ruta_final,
                    "participante_riverside": nombre,
                    "recording_nombre":       nombre_gr,
                })
            except Exception as e:
                print(f"\r       ✗ Error {nombre_archivo}: {e}")

    return archivos_descargados


# ══════════════════════════════════════════════════════
#   MÓDULO 3 — Google Drive (local)
# ══════════════════════════════════════════════════════

def buscar_o_crear_carpeta_raw(ep_folder: Path, programa: str, episodio: str) -> Path:
    for sub in ep_folder.iterdir():
        if sub.is_dir() and "raw" in sub.name.lower():
            return sub
    nombre_raw = f"{programa}_#{episodio}_Raw"
    raw_folder = ep_folder / nombre_raw
    raw_folder.mkdir(exist_ok=True)
    print(f"  + Carpeta RAW creada: {nombre_raw}")
    return raw_folder

def buscar_o_crear_carpeta_local(programa: str, episodio: str) -> Path | None:
    """
    Devuelve (y crea si no existe) la carpeta Media local del episodio.
    Ej: C:\\DaVinci Projects\\CMB\\469\\Media\\
    """
    show_folder   = LOCAL_BASE / LOCAL_SHOW_FOLDERS.get(programa, programa)
    ep_folder     = show_folder / episodio
    media_folder  = ep_folder / "Media"
    timeline_folder = ep_folder / "Timeline"
    try:
        media_folder.mkdir(parents=True, exist_ok=True)
        timeline_folder.mkdir(parents=True, exist_ok=True)
        return media_folder
    except Exception as e:
        print(f"  ✗ No se pudo crear carpeta local: {ep_folder}\n    {e}")
        return None


def buscar_carpeta_episodio(programa: str, episodio: str):
    patron      = f"#{episodio}"
    nombre_carp = DRIVE_FOLDERS.get(programa, SHOWS[programa])
    show_folder = DRIVE_BASE / nombre_carp

    if show_folder.exists():
        for carpeta in show_folder.rglob("*"):
            if carpeta.is_dir() and patron in carpeta.name:
                return carpeta
        print(f"  ✗ No se encontro carpeta con '{patron}' en {show_folder.name}.")
        return None

    print(f"  ⚠  No se encontro la carpeta del show: {show_folder}")
    print(f"  → Buscando '{patron}' en toda la unidad compartida...")
    if not DRIVE_BASE.exists():
        print(f"  ✗ Unidad no encontrada: {DRIVE_BASE}")
        return None
    for top in DRIVE_BASE.iterdir():
        if not top.is_dir(): continue
        for carpeta in top.rglob("*"):
            if carpeta.is_dir() and patron in carpeta.name:
                return carpeta
    return None


# ══════════════════════════════════════════════════════
#   HELPER
# ══════════════════════════════════════════════════════

def limpiar(texto: str) -> str:
    return texto.strip().replace(" ", "_")

def _parsear_nombre_descargado(filename: str) -> tuple[str, str]:
    """
    Extrae participante y toma del nombre del archivo descargado.
    Ejemplos:
      'Deborah_Pardes_Take02.wav' → ('Deborah', '2')
      'Yael_Wissner-Levy_1.wav'  → ('Yael', '1')
      'Yonatan_4.wav'            → ('Yonatan', '4')
      'Dan.wav'                  → ('Dan', '')
    """
    stem = Path(filename).stem                       # sin extensión

    # 1) Patrón explícito: _Take03, _take2, etc.
    m = re.search(r'_[Tt]ake(\d+)$', stem)
    if m:
        nombre_base = stem[:m.start()]
        toma        = str(int(m.group(1)))           # '02' → '2'
    else:
        # 2) Patrón posicional: _1, _4, etc. al final
        m2 = re.search(r'_(\d+)$', stem)
        if m2:
            nombre_base = stem[:m2.start()]
            toma        = str(int(m2.group(1)))
        else:
            nombre_base = stem
            toma        = ""

    # Solo primer nombre (antes del primer guión bajo)
    participante = nombre_base.split("_")[0]
    return participante, toma


# ══════════════════════════════════════════════════════
#   MÓDULO H — Modo headless (--headless)
# ══════════════════════════════════════════════════════

def _encontrar_recordings_episodio(recordings: list, episode: str,
                                   title: str = "", show: str = "") -> list:
    """
    Busca las grabaciones que corresponden a un episodio dado.

    Estrategia por puntaje:
      +10  → el nombre contiene el número de episodio exacto (ej. "474")
      +3   → contiene el apellido del invitado ("Goldberg")
      +2   → contiene palabras clave del título ("Iran", "War")
      +1   → contiene un nombre de host conocido del show ("Yonatan", "Yael")

    Si hay matches → retorna los mejores.
    Si no → fallback a las 3 más recientes.
    """
    if not recordings:
        return []

    # ── Extraer pistas del título ──────────────────────────────────────────────
    pistas_guest  = []
    pistas_titulo = []

    if title:
        # Invitado: "with Rich Goldberg" → apellido + nombre completo
        m_guest = re.search(r'\bwith\s+(.+?)(?:\s*[-–]|$)', title, re.IGNORECASE)
        if m_guest:
            guest_full = m_guest.group(1).strip()
            partes = guest_full.split()
            if partes:
                pistas_guest.append(partes[-1])       # apellido
            if len(partes) > 1:
                pistas_guest.append(guest_full)       # nombre completo

        # Palabras del título (>4 chars, no stopwords)
        STOPWORDS = {
            "with", "the", "and", "for", "from", "that", "this", "what",
            "why", "how", "are", "iran", "call", "back", "inside", "episode",
        }
        palabras      = re.findall(r'\b[A-Za-z]{4,}\b', title)
        pistas_titulo = [p for p in palabras if p.lower() not in STOPWORDS][:4]

    # ── Hosts conocidos del show (pista débil de desempate) ────────────────────
    hosts_show = SHOW_HOSTS.get(show.upper(), []) if show else []

    # ── Puntuar cada recording ─────────────────────────────────────────────────
    scored = []
    for rec in recordings:
        nombre = (rec.get("name") or rec.get("title") or "").lower()
        score  = 0

        if episode and re.search(r'\b' + re.escape(episode) + r'\b', nombre):
            score += 10
        for pista in pistas_guest:
            if pista.lower() in nombre:
                score += 3
        for palabra in pistas_titulo:
            if palabra.lower() in nombre:
                score += 2
        for host in hosts_show:
            if host.lower() in nombre:
                score += 1

        if score > 0:
            scored.append((score, rec))

    if scored:
        scored.sort(key=lambda x: x[0], reverse=True)
        mejor = scored[0][0]

        if mejor >= 10:
            # ── Hay match por número de episodio → puede haber varios takes ──
            # Tomar todas las grabaciones con puntaje alto (todos los takes del ep)
            resultado = [rec for sc, rec in scored if sc >= mejor - 2]
            print(f"  → Match por número de episodio: {len(resultado)} grabación(es)")
            for sc, rec in scored[:3]:
                print(f"       score={sc}  →  {rec.get('name') or rec.get('title')}")
            return resultado[:5]
        else:
            # ── Solo match débil (sin número de ep, ej. WYN) ─────────────────
            # Agarramos la más reciente + todas las tomas del MISMO DÍA
            # con el mismo nombre base (ej. "Yonatan & Yael — Take 02",
            # "Yonatan & Yael — Take 03" y "Yonatan & Yael" son la misma sesión).
            rec_reciente  = scored[0][1]
            fecha_rec     = (rec_reciente.get("created_date") or
                             rec_reciente.get("created_at") or "")[:10]
            nombre_rec    = rec_reciente.get("name") or rec_reciente.get("title") or ""
            # Nombre base: quitar " — Take XX" del final
            base_rec = re.sub(r'\s*[-–]\s*[Tt]ake\s*\d+\s*$', '', nombre_rec).strip().lower()

            takes = []
            for rec in recordings:
                nombre = rec.get("name") or rec.get("title") or ""
                fecha  = (rec.get("created_date") or rec.get("created_at") or "")[:10]
                base   = re.sub(r'\s*[-–]\s*[Tt]ake\s*\d+\s*$', '', nombre).strip().lower()
                if base == base_rec and fecha == fecha_rec:
                    takes.append(rec)

            print(f"  → Match débil (sin número de ep). Sesión del {fecha_rec}: {len(takes)} toma(s)")
            for t in takes:
                print(f"       →  {t.get('name') or t.get('title')}")
            return takes if takes else [rec_reciente]

    # ── Fallback total: ningún match ───────────────────────────────────────────
    print(f"  ⚠  Sin match. Usando la grabación más reciente como fallback.")
    return recordings[:1]


def _run_headless(show: str, episode: str,
                  title: str = "",
                  audio: bool = True, video: bool = False,
                  drive: bool = True, local: bool = True,
                  count: int = 3) -> bool:
    """
    Descarga, renombra y copia sin GUI.
    Usa matching inteligente por episodio/título para encontrar la grabación correcta.

    Uso:
      python ark_riverside_flow.py --headless --show CMB --episode 474
      python ark_riverside_flow.py --headless --show CMB --episode 474 --title "Iran War - with Rich Goldberg"
      python ark_riverside_flow.py --headless --show CMB --episode 474 --no-drive
    """
    print(f"\n{'='*56}")
    print(f"  ARK MEDIA — Modo automático")
    print(f"  Show: {show}  |  Episodio: {episode}")
    if title:
        print(f"  Título:  {title}")
    print(f"  Audio: {'sí' if audio else 'no'}  |  Video: {'sí' if video else 'no'}")
    print(f"  Drive: {'sí' if drive else 'no'}  |  DaVinci: {'sí' if local else 'no'}")
    print(f"{'='*56}\n")

    config = cargar_config()
    aplicar_config_carpetas(config)
    api_key    = config["riverside_api_key"]
    studio_ids = config.get("studio_ids", {})
    studio_id  = studio_ids.get(show)

    if not studio_id:
        print(f"  ✗ No hay studio_id para '{show}'. Corré --setup primero.")
        return False

    # ── Obtener recordings ─────────────────────────────────────────────────────
    print(f"  → Buscando grabaciones del show {show}...")
    recs = _obtener_recordings(api_key, studio_id=studio_id)
    originales = [r for r in recs if not _es_export(r)]
    originales.sort(
        key=lambda r: r.get("created_date") or r.get("created_at") or "9999",
        reverse=True)

    if not originales:
        print(f"  ✗ No se encontraron grabaciones originales.")
        return False

    # ── Matching inteligente por episodio + título/invitado + hosts del show ──
    recs_a_descargar = _encontrar_recordings_episodio(originales, episode, title, show)

    # ── Carpeta temporal ───────────────────────────────────────────────────────
    carpeta_descarga = BASE_DIR / "riverside_downloads" / f"{show}_{episode}"
    carpeta_descarga.mkdir(parents=True, exist_ok=True)

    # ── Descargar tracks ───────────────────────────────────────────────────────
    multiples = len(recs_a_descargar) > 1
    archivos  = []
    for idx, rec in enumerate(recs_a_descargar, 1):
        nombre_gr = rec.get("name") or rec.get("title") or ""
        sufijo    = _sufijo_toma(nombre_gr, idx) if multiples else ""
        print(f"\n  Recording {idx}/{len(recs_a_descargar)}: {nombre_gr}")
        archivos_rec = _descargar_tracks(
            rec, carpeta_descarga, api_key,
            sufijo=sufijo, bajar_audio=audio, bajar_video=video)
        archivos.extend(archivos_rec)

    if not archivos:
        print(f"\n  ✗ No se descargaron archivos.")
        return False

    print(f"\n  → {len(archivos)} archivo(s) descargados. Renombrando...")

    # ── Auto-renombrar ─────────────────────────────────────────────────────────
    plan = []
    for info in archivos:
        archivo            = info["ruta"]
        participante, toma = _parsear_nombre_descargado(archivo.name)
        participante       = limpiar(participante) or "Participante"
        nuevo_nombre = (
            f"{show}_{episode}_{participante}_{toma}{archivo.suffix}"
            if toma else
            f"{show}_{episode}_{participante}{archivo.suffix}"
        )
        print(f"  → {archivo.name}  →  {nuevo_nombre}")
        plan.append((archivo, nuevo_nombre))

    # ── Resolver destinos ──────────────────────────────────────────────────────
    drive_ok = local_ok = False
    raw_folder = local_folder = None

    if drive:
        if DRIVE_BASE.exists():
            ep_folder = buscar_carpeta_episodio(show, episode)
            if ep_folder:
                raw_folder = buscar_o_crear_carpeta_raw(ep_folder, show, episode)
                drive_ok   = True
            else:
                print(f"  ⚠  Drive: carpeta '#{episode}' no encontrada. Se omite Drive.")
        else:
            print(f"  ⚠  Drive no disponible ({DRIVE_BASE}). Se omite Drive.")

    if local:
        local_folder = buscar_o_crear_carpeta_local(show, episode)
        local_ok     = local_folder is not None

    if not drive_ok and not local_ok:
        print(f"  ✗ No se pudo acceder a ningún destino.")
        return False

    # ── Copiar archivos ────────────────────────────────────────────────────────
    carpeta_final  = carpeta_descarga / "listos"
    carpeta_final.mkdir(exist_ok=True)
    copiados_drive = copiados_local = errores_count = 0

    for archivo, nuevo_nombre in plan:
        nueva_ruta = carpeta_final / nuevo_nombre
        try:
            shutil.copy2(archivo, nueva_ruta)
        except Exception as e:
            print(f"  ✗ Rename {nuevo_nombre}: {e}")
            errores_count += 1
            continue

        if drive_ok and raw_folder:
            try:
                shutil.copy2(nueva_ruta, raw_folder / nuevo_nombre)
                copiados_drive += 1
                print(f"  ✓ {nuevo_nombre}  →  Drive/RAW")
            except Exception as e:
                print(f"  ✗ Drive {nuevo_nombre}: {e}")
                errores_count += 1

        if local_ok and local_folder:
            try:
                shutil.copy2(nueva_ruta, local_folder / nuevo_nombre)
                copiados_local += 1
                print(f"  ✓ {nuevo_nombre}  →  DaVinci/Media")
            except Exception as e:
                print(f"  ✗ DaVinci {nuevo_nombre}: {e}")
                errores_count += 1

    # ── Cleanup ────────────────────────────────────────────────────────────────
    if errores_count == 0:
        try:
            shutil.rmtree(carpeta_descarga)
            print(f"  🗑  Carpeta temporal eliminada.")
        except Exception as e:
            print(f"  ⚠  No se pudo eliminar carpeta temporal: {e}")

    print(f"\n{'='*56}")
    print(f"  ✓ Drive: {copiados_drive}  |  DaVinci: {copiados_local}  |  Errores: {errores_count}")
    print(f"{'='*56}\n")
    return errores_count == 0


# ══════════════════════════════════════════════════════
#   GUI — Ventana de escritorio
# ══════════════════════════════════════════════════════

# Importar tkinter solo cuando se necesita la GUI.
# En modo headless/setup se usan stubs mínimos para que las definiciones
# de clase no fallen — nunca se instancian en esos modos.
_GUI_MODE = "--headless" not in sys.argv and "--setup" not in sys.argv

if _GUI_MODE:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog, font as tkfont
else:
    import types as _types
    tk = _types.SimpleNamespace(
        Tk=object, Frame=object, Toplevel=object, Canvas=object,
        Text=object, Checkbutton=object, Scrollbar=object, Menu=object,
        BooleanVar=lambda *a, **k: None, StringVar=lambda *a, **k: None,
        IntVar=lambda *a, **k: None,
    )
    ttk = _types.SimpleNamespace(
        Frame=object, Label=object, Button=object, Entry=object,
        Combobox=object, Checkbutton=object, Progressbar=object,
        Style=lambda *a, **k: None, Notebook=object,
    )
    messagebox = _types.SimpleNamespace(
        showwarning=lambda *a, **k: None, showerror=lambda *a, **k: None,
        showinfo=lambda *a, **k: None, askyesnocancel=lambda *a, **k: None,
    )
    filedialog = _types.SimpleNamespace(askdirectory=lambda *a, **k: "")
    tkfont     = _types.SimpleNamespace(Font=lambda *a, **k: None, families=lambda: [])

# ── Idioma ────────────────────────────────────────────────────────────────────

_LANG = "es"

TRANSLATIONS = {
    "es": {
        "subtitle":          "Riverside → Drive",
        "btn_config":        "⚙  Configuración",
        "btn_lang":          "🌐  EN",
        "card1":             "1 — Show y episodio",
        "lbl_show":          "Show:",
        "lbl_episode":       "Episodio:",
        "btn_search":        "Buscar grabaciones",
        "searching":         "Buscando grabaciones…",
        "search_none":       "✗ No se encontraron grabaciones originales.",
        "search_ok":         "✓ {total} grabaciones originales (mostrando las {shown} más recientes)",
        "card2":             "2 — Grabaciones originales",
        "list_hint":         "Ingresá show + episodio y presioná «Buscar grabaciones»",
        "col_recording":     "Grabación",
        "col_date":          "Fecha",
        "btn_sel":           "Seleccionar todo",
        "btn_desel":         "Deseleccionar todo",
        "btn_download":      "⬇  Descargar seleccionadas",
        "card3":             "3 — Descarga",
        "dl_starting":       "Iniciando…",
        "dl_done":           "✓ {n} archivo(s) descargados",
        "card4":             "4 — Renombrar archivos",
        "rename_hint":       "(Los archivos descargados aparecerán aquí para renombrar)",
        "col_original":      "Archivo original",
        "col_participant":   "Participante",
        "col_take":          "Toma",
        "col_final":         "Nombre final",
        "chk_drive":         "☁  Google Drive",
        "chk_local":         "💻  DaVinci Local",
        "btn_copy":          "⬆  Copiar",
        "card_log":          "Log",
        # Mensajes
        "warn_no_ep":        "Ingresá el número de episodio.",
        "warn_no_sel":       "Seleccioná al menos una grabación.",
        "warn_no_format":    "Seleccioná al menos Audio o Video antes de descargar.",
        "warn_no_dest":      "Seleccioná al menos un destino (Drive o DaVinci).",
        "err_no_dest":       "No se pudo acceder a ningún destino.\nVerificá las rutas en ⚙ Configuración.",
        "dup_title":         "Archivos ya existentes en Drive",
        "dup_msg":           "Los siguientes archivos ya están en Drive/RAW:\n\n{lista}\n\n¿Sobreescribir?  (No = saltear solo esos archivos)",
        "copy_done_title":   "¡Listo!",
        "copy_done":         "✓ Archivos copiados:\n\n{resumen}",
        "copy_errors_title": "Completado con errores",
        # Config dialog
        "cfg_title":         "Configuración — ARK MEDIA",
        "cfg_subtitle":      "Los cambios se guardan en config.json",
        "cfg_drive_card":    "📁  Unidad de Google Drive",
        "cfg_drive_root":    "Carpeta raíz compartida:",
        "cfg_drive_shows":   "📂  Subcarpetas por show (dentro de la raíz)",
        "cfg_shows_hint":    "Nombre exacto de la carpeta del show dentro de la raíz.",
        "cfg_browse_tip":    "💡  «Examinar» abre el selector — el nombre relativo se calcula automáticamente.",
        "cfg_local_card":    "💻  Carpeta local (DaVinci Resolve)",
        "cfg_local_root":    "Carpeta raíz de proyectos DaVinci:",
        "cfg_local_shows":   "Subcarpeta por show (dentro de la raíz):",
        "cfg_local_tip":     "💡  Los archivos van a: raíz / show / episodio / Media /",
        "cfg_advanced":      "▶  Opciones avanzadas (admin)",
        "cfg_advanced_open": "▼  Opciones avanzadas (admin)",
        "cfg_api_card":      "🔑  API Key de Riverside",
        "btn_browse":        "Examinar…",
        "btn_cancel":        "Cancelar",
        "btn_save":          "💾  Guardar",
        "saved_title":       "Guardado",
        "saved_ok":          "✓ Configuración guardada.",
        "err_api_missing":   "Ingresá la API key.",
    },
    "en": {
        "subtitle":          "Riverside → Drive",
        "btn_config":        "⚙  Settings",
        "btn_lang":          "🌐  ES",
        "card1":             "1 — Show & episode",
        "lbl_show":          "Show:",
        "lbl_episode":       "Episode:",
        "btn_search":        "Search recordings",
        "searching":         "Searching recordings…",
        "search_none":       "✗ No original recordings found.",
        "search_ok":         "✓ {total} original recordings (showing {shown} most recent)",
        "card2":             "2 — Original recordings",
        "list_hint":         "Enter show + episode and press «Search recordings»",
        "col_recording":     "Recording",
        "col_date":          "Date",
        "btn_sel":           "Select all",
        "btn_desel":         "Deselect all",
        "btn_download":      "⬇  Download selected",
        "card3":             "3 — Download",
        "dl_starting":       "Starting…",
        "dl_done":           "✓ {n} file(s) downloaded",
        "card4":             "4 — Rename files",
        "rename_hint":       "(Downloaded files will appear here for renaming)",
        "col_original":      "Original file",
        "col_participant":   "Participant",
        "col_take":          "Take",
        "col_final":         "Final name",
        "chk_drive":         "☁  Google Drive",
        "chk_local":         "💻  DaVinci Local",
        "btn_copy":          "⬆  Copy",
        "card_log":          "Log",
        # Messages
        "warn_no_ep":        "Enter the episode number.",
        "warn_no_sel":       "Select at least one recording.",
        "warn_no_format":    "Select at least Audio or Video before downloading.",
        "warn_no_dest":      "Select at least one destination (Drive or DaVinci).",
        "err_no_dest":       "Could not access any destination.\nCheck paths in ⚙ Settings.",
        "dup_title":         "Files already exist in Drive",
        "dup_msg":           "The following files already exist in Drive/RAW:\n\n{lista}\n\nOverwrite?  (No = skip those files only)",
        "copy_done_title":   "Done!",
        "copy_done":         "✓ Files copied:\n\n{resumen}",
        "copy_errors_title": "Completed with errors",
        # Config dialog
        "cfg_title":         "Settings — ARK MEDIA",
        "cfg_subtitle":      "Changes are saved to config.json",
        "cfg_drive_card":    "📁  Google Drive folder",
        "cfg_drive_root":    "Shared root folder:",
        "cfg_drive_shows":   "📂  Show subfolders (inside root)",
        "cfg_shows_hint":    "Exact name of the show folder inside the root.",
        "cfg_browse_tip":    "💡  «Browse» opens the folder picker — the relative name is calculated automatically.",
        "cfg_local_card":    "💻  Local folder (DaVinci Resolve)",
        "cfg_local_root":    "DaVinci projects root folder:",
        "cfg_local_shows":   "Show subfolder (inside root):",
        "cfg_local_tip":     "💡  Files go to: root / show / episode / Media /",
        "cfg_advanced":      "▶  Advanced options (admin)",
        "cfg_advanced_open": "▼  Advanced options (admin)",
        "cfg_api_card":      "🔑  Riverside API Key",
        "btn_browse":        "Browse…",
        "btn_cancel":        "Cancel",
        "btn_save":          "💾  Save",
        "saved_title":       "Saved",
        "saved_ok":          "✓ Settings saved.",
        "err_api_missing":   "Enter the API key.",
    },
}


def _t(key: str, **kw) -> str:
    s = TRANSLATIONS.get(_LANG, TRANSLATIONS["es"]).get(key, key)
    return s.format(**kw) if kw else s


# ── Paleta de colores
BG       = "#ECEEF2"   # fondo general (gris azulado suave)
PANEL    = "#FFFFFF"   # tarjetas blancas
ACCENT   = "#4F46E5"   # índigo
ACCENT_H = "#3730A3"   # hover índigo oscuro
TEXT     = "#111827"   # texto principal casi negro
MUTED    = "#6B7280"   # texto secundario gris
GREEN    = "#059669"   # verde éxito
RED      = "#DC2626"   # rojo error
BORDER   = "#D1D5DB"   # bordes


class _Card(ttk.Frame):
    """Frame con fondo blanco y borde sutil — 'tarjeta'."""
    def __init__(self, parent, title="", **kw):
        super().__init__(parent, style="Card.TFrame", **kw)
        self._title_label = None
        if title:
            self._title_label = ttk.Label(self, text=title, style="CardTitle.TLabel")
            self._title_label.pack(anchor="w", padx=16, pady=(12, 4))


class _ConfigDialog(tk.Toplevel):
    """
    Ventana de configuración: API key, studio IDs y rutas de Google Drive.
    Los cambios se guardan en config.json y se aplican de inmediato.
    """

    def __init__(self, parent, config: dict):
        super().__init__(parent)
        self.parent  = parent
        self.cfg     = config          # referencia al dict vivo de ArkApp
        self.title(_t("cfg_title"))
        self.geometry("640x580")
        self.resizable(True, True)
        self.configure(bg=BG)
        self.grab_set()                # modal
        self.transient(parent)

        self._build()

    # ── Layout ────────────────────────────────────────────────────────────────

    def _build(self):
        # Título
        ttk.Label(self, text=_t("cfg_title").split("—")[0].strip(),
                  style="Header.TLabel").pack(anchor="w", padx=24, pady=(20, 2))
        ttk.Label(self, text=_t("cfg_subtitle"),
                  style="Muted2.TLabel").pack(anchor="w", padx=24, pady=(0, 12))

        PAD = {"padx": 24, "pady": 6}

        # ── Carpeta base de Google Drive ──────────────────────────────────────
        c1 = _Card(self, title=_t("cfg_drive_card"))
        c1.pack(fill="x", **PAD)
        c1.configure(borderwidth=1, relief="solid")

        ttk.Label(c1, text=_t("cfg_drive_root"), style="Muted.TLabel").pack(
            anchor="w", padx=16, pady=(0, 4))
        row_base = ttk.Frame(c1, style="Card.TFrame")
        row_base.pack(fill="x", padx=16, pady=(0, 12))
        self.var_drive_base = tk.StringVar(value=str(DRIVE_BASE))
        ttk.Entry(row_base, textvariable=self.var_drive_base, width=52).pack(
            side="left", padx=(0, 8))
        ttk.Button(row_base, text=_t("btn_browse"), style="Small.TButton",
                   command=lambda: self._browse(self.var_drive_base)).pack(side="left")

        # ── Subcarpetas por show ──────────────────────────────────────────────
        c2 = _Card(self, title=_t("cfg_drive_shows"))
        c2.pack(fill="x", **PAD)
        c2.configure(borderwidth=1, relief="solid")

        ttk.Label(c2, text=_t("cfg_shows_hint"),
                  style="Muted.TLabel").pack(anchor="w", padx=16, pady=(0, 8))

        self.var_folders = {}
        for codigo, nombre_show in SHOWS.items():
            row = ttk.Frame(c2, style="Card.TFrame")
            row.pack(fill="x", padx=16, pady=(0, 6))
            ttk.Label(row, text=f"{codigo} — {nombre_show}:",
                      style="Card.TLabel", width=26, anchor="w").pack(side="left")
            v = tk.StringVar(value=DRIVE_FOLDERS.get(codigo, ""))
            self.var_folders[codigo] = v
            ttk.Entry(row, textvariable=v, width=32).pack(side="left", padx=(0, 8))
            ttk.Button(row, text=_t("btn_browse"), style="Small.TButton",
                       command=lambda vv=v: self._browse_subfolder(vv)).pack(side="left")

        ttk.Label(c2, text=_t("cfg_browse_tip"),
                  style="Muted.TLabel").pack(anchor="w", padx=16, pady=(0, 10))

        # ── Carpeta base local (DaVinci) ──────────────────────────────────────
        c3 = _Card(self, title=_t("cfg_local_card"))
        c3.pack(fill="x", **PAD)
        c3.configure(borderwidth=1, relief="solid")

        ttk.Label(c3, text=_t("cfg_local_root"), style="Muted.TLabel").pack(
            anchor="w", padx=16, pady=(0, 4))
        row_local = ttk.Frame(c3, style="Card.TFrame")
        row_local.pack(fill="x", padx=16, pady=(0, 8))
        self.var_local_base = tk.StringVar(value=str(LOCAL_BASE))
        ttk.Entry(row_local, textvariable=self.var_local_base, width=52).pack(
            side="left", padx=(0, 8))
        ttk.Button(row_local, text=_t("btn_browse"), style="Small.TButton",
                   command=lambda: self._browse(self.var_local_base)).pack(side="left")

        ttk.Label(c3, text=_t("cfg_local_shows"),
                  style="Muted.TLabel").pack(anchor="w", padx=16, pady=(0, 6))

        self.var_local_folders = {}
        for codigo, nombre_show in SHOWS.items():
            row = ttk.Frame(c3, style="Card.TFrame")
            row.pack(fill="x", padx=16, pady=(0, 4))
            ttk.Label(row, text=f"{codigo} — {nombre_show}:",
                      style="Card.TLabel", width=26, anchor="w").pack(side="left")
            v = tk.StringVar(value=LOCAL_SHOW_FOLDERS.get(codigo, codigo))
            self.var_local_folders[codigo] = v
            ttk.Entry(row, textvariable=v, width=32).pack(side="left", padx=(0, 8))
            ttk.Button(row, text=_t("btn_browse"), style="Small.TButton",
                       command=lambda vv=v, base_var=self.var_local_base:
                           self._browse_subfolder(vv, base_var)).pack(side="left")

        ttk.Label(c3, text=_t("cfg_local_tip"),
                  style="Muted.TLabel").pack(anchor="w", padx=16, pady=(4, 10))

        # ── API Key (sección admin colapsable) ────────────────────────────────
        self._admin_visible = False
        self.var_api = tk.StringVar(value=self.cfg.get("riverside_api_key", ""))
        self._admin_frame = None

        admin_toggle = ttk.Frame(self)
        admin_toggle.pack(fill="x", padx=24, pady=(0, 4))
        self._btn_admin = ttk.Button(admin_toggle, text=_t("cfg_advanced"),
                                     style="Small.TButton",
                                     command=self._toggle_admin)
        self._btn_admin.pack(anchor="w")

        # Contenedor que se muestra/oculta
        self._admin_container = ttk.Frame(self)
        # (no se hace pack todavía — se muestra al hacer click)

        # ── Botones ───────────────────────────────────────────────────────────
        self._btn_row = ttk.Frame(self)
        self._btn_row.pack(fill="x", padx=24, pady=(8, 20))
        ttk.Button(self._btn_row, text=_t("btn_cancel"), style="Small.TButton",
                   command=self.destroy).pack(side="right", padx=(6, 0))
        ttk.Button(self._btn_row, text=_t("btn_save"), style="Accent.TButton",
                   command=self._guardar).pack(side="right")

    def _toggle_admin(self):
        self._admin_visible = not self._admin_visible
        if self._admin_visible:
            self._btn_admin.configure(text=_t("cfg_advanced_open"))
            # Construir la sección si es la primera vez
            if not self._admin_frame:
                c3 = _Card(self._admin_container, title=_t("cfg_api_card"))
                c3.pack(fill="x", padx=0, pady=0)
                c3.configure(borderwidth=1, relief="solid")
                row_api = ttk.Frame(c3, style="Card.TFrame")
                row_api.pack(fill="x", padx=16, pady=(0, 12))
                e = ttk.Entry(row_api, textvariable=self.var_api, width=56, show="•")
                e.pack(side="left", padx=(0, 8))
                self._api_entry = e
                ttk.Button(row_api, text="👁", style="Small.TButton",
                           command=self._toggle_api_vis).pack(side="left")
                self._api_visible = False
                self._admin_frame = c3
            # Insertar antes de los botones
            self._admin_container.pack(fill="x", padx=24, pady=(0, 6),
                                       before=self._btn_row)
        else:
            self._btn_admin.configure(text=_t("cfg_advanced"))
            self._admin_container.pack_forget()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _browse(self, var: tk.StringVar):
        """Abre un selector de carpeta y pone el path absoluto en var."""
        inicial = var.get() if Path(var.get()).exists() else str(Path.home())
        path = filedialog.askdirectory(
            title="Seleccionar carpeta",
            initialdir=inicial,
            parent=self)
        if path:
            var.set(str(Path(path)))

    def _browse_subfolder(self, var: tk.StringVar, base_var: tk.StringVar = None):
        """
        Abre un selector de carpeta dentro de la raíz indicada.
        Guarda solo el nombre relativo. Si base_var es None, usa var_drive_base.
        """
        base_str = (base_var or self.var_drive_base).get()
        base     = Path(base_str)
        inicial  = str(base) if base.exists() else str(Path.home())
        path = filedialog.askdirectory(
            title="Seleccionar carpeta del show",
            initialdir=inicial,
            parent=self)
        if path:
            selected = Path(path)
            try:
                rel = selected.relative_to(base)
                var.set(str(rel))
            except ValueError:
                var.set(str(selected))

    def _toggle_api_vis(self):
        self._api_visible = not self._api_visible
        if hasattr(self, "_api_entry"):
            self._api_entry.configure(show="" if self._api_visible else "•")

    def _guardar(self):
        global DRIVE_BASE, DRIVE_FOLDERS, LOCAL_BASE, LOCAL_SHOW_FOLDERS

        api_key    = self.var_api.get().strip()
        drive_base = self.var_drive_base.get().strip()
        local_base = self.var_local_base.get().strip()

        if not api_key:
            messagebox.showwarning("—", _t("err_api_missing"), parent=self)
            return

        # Actualizar globals Drive
        if drive_base:
            DRIVE_BASE = Path(drive_base)
        for codigo, v in self.var_folders.items():
            val = v.get().strip()
            if val:
                DRIVE_FOLDERS[codigo] = val

        # Actualizar globals locales
        if local_base:
            LOCAL_BASE = Path(local_base)
        for codigo, v in self.var_local_folders.items():
            val = v.get().strip()
            if val:
                LOCAL_SHOW_FOLDERS[codigo] = val

        # Persistir en config.json
        self.cfg["riverside_api_key"]  = api_key
        self.cfg["drive_base"]         = drive_base
        self.cfg["drive_folders"]      = {k: v.get().strip()
                                           for k, v in self.var_folders.items()
                                           if v.get().strip()}
        self.cfg["local_base"]         = local_base
        self.cfg["local_show_folders"] = {k: v.get().strip()
                                           for k, v in self.var_local_folders.items()
                                           if v.get().strip()}
        guardar_config(self.cfg)

        # Actualizar API key en ArkApp
        self.parent.api_key = api_key

        messagebox.showinfo(_t("saved_title"), _t("saved_ok"), parent=self)
        self.destroy()


class ArkApp(tk.Tk):

    def __init__(self, config: dict):
        super().__init__()
        self.config_data = config
        self.api_key     = config["riverside_api_key"]

        # Estado
        self.recordings           = []
        self.check_vars           = []
        self.archivos_descargados = []
        self.carpeta_descarga     = None
        self._log_queue           = q_module.Queue()
        self._dl_queue            = q_module.Queue()
        self._tw                  = []   # [(widget, attr, key, kw)] para traducciones

        self.title("ARK MEDIA — Riverside Flow")
        self.geometry("820x900")
        self.configure(bg=BG)
        self.resizable(True, True)
        self.minsize(640, 700)

        self._estilos()
        self._build()
        self._redirigir_stdout()
        self._poll_log()

    # ── Idioma ────────────────────────────────────────────────────────────────

    def _reg(self, widget, key: str, attr: str = "text", **kw):
        """Registra un widget para actualizarse al cambiar idioma."""
        self._tw.append((widget, attr, key, kw))
        return widget

    def _apply_lang(self):
        for w, attr, key, kw in self._tw:
            try:
                w.configure(**{attr: _t(key, **kw)})
            except Exception:
                pass
        # Actualizar el título de la ventana
        self.title("ARK MEDIA — Riverside Flow")

    def _toggle_lang(self):
        global _LANG
        _LANG = "en" if _LANG == "es" else "es"
        self._apply_lang()

    # ── Estilos ───────────────────────────────────────────────────────────────

    def _estilos(self):
        s = ttk.Style(self)
        s.theme_use("clam")   # clam respeta colores personalizados en Windows

        # Base
        s.configure(".",
                    background=BG, foreground=TEXT,
                    font=(FONT_UI, 10), borderwidth=0)
        s.configure("TFrame",           background=BG)
        s.configure("Card.TFrame",      background=PANEL)

        # Labels
        s.configure("TLabel",           background=BG,    foreground=TEXT)
        s.configure("Card.TLabel",      background=PANEL, foreground=TEXT)
        s.configure("CardTitle.TLabel", background=PANEL, foreground=TEXT,
                    font=(FONT_UI, 10, "bold"))
        s.configure("Muted.TLabel",     background=PANEL, foreground=MUTED,
                    font=(FONT_UI, 9))
        s.configure("Muted2.TLabel",    background=BG,    foreground=MUTED,
                    font=(FONT_UI, 9))
        s.configure("Green.TLabel",     background=PANEL, foreground=GREEN,
                    font=(FONT_UI, 9, "bold"))
        s.configure("Header.TLabel",    background=BG,    foreground=TEXT,
                    font=(FONT_UI, 16, "bold"))
        s.configure("Sub.TLabel",       background=BG,    foreground=MUTED,
                    font=(FONT_UI, 10))

        # Botón principal (índigo + texto blanco garantizado)
        s.configure("Accent.TButton",
                    background=ACCENT, foreground="#FFFFFF",
                    bordercolor=ACCENT, darkcolor=ACCENT, lightcolor=ACCENT,
                    font=(FONT_UI, 10, "bold"), padding=(14, 7), relief="flat")
        s.map("Accent.TButton",
              background=[("active",   ACCENT_H), ("pressed",  ACCENT_H),
                          ("disabled", BORDER),   ("!active",  ACCENT)],
              foreground=[("active",   "#FFFFFF"), ("pressed",  "#FFFFFF"),
                          ("disabled", MUTED),     ("!active",  "#FFFFFF")],
              bordercolor=[("active", ACCENT_H)])

        # Botón secundario (gris claro + texto oscuro)
        s.configure("Small.TButton",
                    background="#E5E7EB", foreground=TEXT,
                    bordercolor="#E5E7EB", darkcolor="#E5E7EB", lightcolor="#E5E7EB",
                    font=(FONT_UI, 9), padding=(8, 4), relief="flat")
        s.map("Small.TButton",
              background=[("active", BORDER), ("!active", "#E5E7EB")],
              foreground=[("active", TEXT),   ("!active", TEXT)])

        # Inputs
        s.configure("TEntry",
                    fieldbackground=PANEL, foreground=TEXT,
                    bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER,
                    padding=6)
        s.configure("TCombobox",
                    fieldbackground=PANEL, foreground=TEXT,
                    bordercolor=BORDER, arrowcolor=TEXT)
        s.map("TCombobox",
              fieldbackground=[("readonly", PANEL)],
              foreground=[("readonly", TEXT)])

        # Progressbar
        s.configure("TProgressbar",
                    troughcolor=BORDER, background=ACCENT,
                    thickness=10, borderwidth=0, relief="flat")

        # Scrollbar
        s.configure("TScrollbar",
                    background=BORDER, troughcolor=BG,
                    arrowcolor=MUTED, borderwidth=0)

    # ── Layout ────────────────────────────────────────────────────────────────

    def _build(self):
        # ── Encabezado ────────────────────────────────────────────────────────
        hdr = ttk.Frame(self)
        hdr.pack(fill="x", padx=24, pady=(20, 4))
        ttk.Label(hdr, text="ARK MEDIA", style="Header.TLabel").pack(side="left")
        self._reg(ttk.Label(hdr, text=_t("subtitle"), style="Sub.TLabel"),
                  "subtitle").pack(side="left", pady=(6, 0))
        self._reg(ttk.Button(hdr, text=_t("btn_config"), style="Small.TButton",
                             command=self._abrir_config), "btn_config").pack(
                             side="right", pady=(4, 0), padx=(6, 0))
        self._lang_btn = ttk.Button(hdr, text=_t("btn_lang"), style="Small.TButton",
                                    command=self._toggle_lang)
        self._reg(self._lang_btn, "btn_lang").pack(side="right", pady=(4, 0))

        # Scrollable main area
        self._canvas = tk.Canvas(self, bg=BG, highlightthickness=0)
        sb = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._canvas.pack(side="left", fill="both", expand=True)

        self._main = ttk.Frame(self._canvas)
        self._win  = self._canvas.create_window((0, 0), window=self._main, anchor="nw")
        self._main.bind("<Configure>", lambda e: (
            self._canvas.configure(scrollregion=self._canvas.bbox("all"))))
        self._canvas.bind("<Configure>", lambda e:
            self._canvas.itemconfig(self._win, width=e.width))
        self._canvas.bind_all("<MouseWheel>", lambda e:
            self._canvas.yview_scroll(self._scroll_delta(e), "units"))

        PAD = {"padx": 24, "pady": 6}

        # ── Paso 1: Show + Episodio ───────────────────────────────────────────
        c1 = _Card(self._main, title=_t("card1"))
        c1.pack(fill="x", **PAD)
        self._card_border(c1)
        self._reg(c1._title_label, "card1")

        row1 = ttk.Frame(c1, style="Card.TFrame")
        row1.pack(fill="x", padx=16, pady=(0, 14))

        self._reg(ttk.Label(row1, text=_t("lbl_show"), style="Card.TLabel"),
                  "lbl_show").grid(row=0, column=0, sticky="w", padx=(0,6))
        self.var_programa = tk.StringVar(value="CMB")
        cb = ttk.Combobox(row1, textvariable=self.var_programa,
                          values=list(SHOWS.keys()), width=8, state="readonly")
        cb.grid(row=0, column=1, sticky="w", padx=(0, 20))

        self._reg(ttk.Label(row1, text=_t("lbl_episode"), style="Card.TLabel"),
                  "lbl_episode").grid(row=0, column=2, sticky="w", padx=(0,6))
        self.var_episodio = tk.StringVar()
        ep = ttk.Entry(row1, textvariable=self.var_episodio, width=10)
        ep.grid(row=0, column=3, sticky="w", padx=(0, 16))
        ep.bind("<Return>", lambda e: self._buscar())

        self.btn_buscar = ttk.Button(row1, text=_t("btn_search"),
                                     style="Accent.TButton", command=self._buscar)
        self._reg(self.btn_buscar, "btn_search")
        self.btn_buscar.grid(row=0, column=4, sticky="w")

        self.lbl_buscar_st = ttk.Label(c1, text="", style="Muted.TLabel")
        self.lbl_buscar_st.pack(anchor="w", padx=16, pady=(0, 10))

        # ── Paso 2: Lista de grabaciones ─────────────────────────────────────
        c2 = _Card(self._main, title=_t("card2"))
        c2.pack(fill="x", **PAD)
        self._card_border(c2)
        self._reg(c2._title_label, "card2")

        # Frame con canvas scrollable para checkboxes
        list_wrap = ttk.Frame(c2, style="Card.TFrame")
        list_wrap.pack(fill="x", padx=16, pady=(0, 8))

        self.list_canvas = tk.Canvas(list_wrap, bg=PANEL, highlightthickness=0,
                                     height=220, bd=0)
        self.list_canvas.pack(side="left", fill="both", expand=True)
        list_sb = ttk.Scrollbar(list_wrap, orient="vertical",
                                command=self.list_canvas.yview)
        list_sb.pack(side="right", fill="y")
        self.list_canvas.configure(yscrollcommand=list_sb.set)

        self.list_frame = ttk.Frame(self.list_canvas, style="Card.TFrame")
        self._list_win  = self.list_canvas.create_window(
            (0, 0), window=self.list_frame, anchor="nw")
        self.list_frame.bind("<Configure>", lambda e: (
            self.list_canvas.configure(scrollregion=self.list_canvas.bbox("all"))))
        self.list_canvas.bind("<Configure>", lambda e:
            self.list_canvas.itemconfig(self._list_win, width=e.width))
        self.list_canvas.bind("<Enter>",
            lambda e: self.list_canvas.bind_all("<MouseWheel>", self._scroll_lista))
        self.list_canvas.bind("<Leave>",
            lambda e: self.list_canvas.bind_all("<MouseWheel>", lambda ev:
                self._canvas.yview_scroll(self._scroll_delta(ev), "units")))

        self.lbl_empty = ttk.Label(self.list_frame,
                                   text=_t("list_hint"), style="Muted.TLabel")
        self._reg(self.lbl_empty, "list_hint")
        self.lbl_empty.pack(pady=20)

        btn_row2 = ttk.Frame(c2, style="Card.TFrame")
        btn_row2.pack(fill="x", padx=16, pady=(0, 12))
        self._reg(ttk.Button(btn_row2, text=_t("btn_sel"), style="Small.TButton",
                             command=self._sel_todo), "btn_sel").pack(side="left", padx=(0, 6))
        self._reg(ttk.Button(btn_row2, text=_t("btn_desel"), style="Small.TButton",
                             command=self._desel_todo), "btn_desel").pack(side="left")

        # Checkboxes Audio / Video
        self.var_audio = tk.BooleanVar(value=True)
        self.var_video = tk.BooleanVar(value=True)
        tk.Checkbutton(btn_row2, text="Audio", variable=self.var_audio,
                       bg=PANEL, activebackground=PANEL, selectcolor=PANEL,
                       font=(FONT_UI, 9), fg=TEXT,
                       relief="flat", bd=0).pack(side="left", padx=(16, 0))
        tk.Checkbutton(btn_row2, text="Video", variable=self.var_video,
                       bg=PANEL, activebackground=PANEL, selectcolor=PANEL,
                       font=(FONT_UI, 9), fg=TEXT,
                       relief="flat", bd=0).pack(side="left", padx=(4, 0))

        self.btn_dl = ttk.Button(btn_row2, text=_t("btn_download"),
                                 style="Accent.TButton", command=self._descargar,
                                 state="disabled")
        self._reg(self.btn_dl, "btn_download")
        self.btn_dl.pack(side="right")

        # ── Paso 3: Progreso ─────────────────────────────────────────────────
        c3 = _Card(self._main, title=_t("card3"))
        c3.pack(fill="x", **PAD)
        self._card_border(c3)
        self._reg(c3._title_label, "card3")

        self.var_prog  = tk.DoubleVar(value=0)
        self.var_prog2 = tk.DoubleVar(value=0)

        prog_wrap = ttk.Frame(c3, style="Card.TFrame")
        prog_wrap.pack(fill="x", padx=16, pady=(0, 4))
        self.lbl_dl_name = ttk.Label(prog_wrap, text="—", style="Muted.TLabel")
        self.lbl_dl_name.pack(anchor="w")
        ttk.Progressbar(prog_wrap, variable=self.var_prog, maximum=100).pack(
            fill="x", pady=(2, 8))

        self.lbl_dl_overall = ttk.Label(prog_wrap, text="", style="Muted.TLabel")
        self.lbl_dl_overall.pack(anchor="w")
        ttk.Progressbar(prog_wrap, variable=self.var_prog2, maximum=100).pack(
            fill="x", pady=(2, 12))

        # ── Paso 4: Renombrar ─────────────────────────────────────────────────
        c4 = _Card(self._main, title=_t("card4"))
        c4.pack(fill="x", **PAD)
        self._card_border(c4)
        self._reg(c4._title_label, "card4")

        self.rename_wrap = ttk.Frame(c4, style="Card.TFrame")
        self.rename_wrap.pack(fill="x", padx=16, pady=(0, 12))
        self._reg(ttk.Label(self.rename_wrap, text=_t("rename_hint"),
                            style="Muted.TLabel"), "rename_hint").pack(pady=10)

        dest_row = ttk.Frame(c4, style="Card.TFrame")
        dest_row.pack(fill="x", padx=16, pady=(4, 0))
        self.var_dest_drive = tk.BooleanVar(value=True)
        self.var_dest_local = tk.BooleanVar(value=True)
        chk_drive = tk.Checkbutton(dest_row, text=_t("chk_drive"),
                                   variable=self.var_dest_drive,
                                   bg=PANEL, activebackground=PANEL, selectcolor=PANEL,
                                   font=(FONT_UI, 9), fg=TEXT, relief="flat", bd=0)
        chk_drive.pack(side="left", padx=(0, 16))
        self._reg(chk_drive, "chk_drive")
        chk_local = tk.Checkbutton(dest_row, text=_t("chk_local"),
                                   variable=self.var_dest_local,
                                   bg=PANEL, activebackground=PANEL, selectcolor=PANEL,
                                   font=(FONT_UI, 9), fg=TEXT, relief="flat", bd=0)
        chk_local.pack(side="left")
        self._reg(chk_local, "chk_local")

        self.btn_drive = ttk.Button(c4, text=_t("btn_copy"),
                                    style="Accent.TButton",
                                    command=self._copiar_drive, state="disabled")
        self._reg(self.btn_drive, "btn_copy")
        self.btn_drive.pack(anchor="e", padx=16, pady=(6, 14))

        # ── Log ───────────────────────────────────────────────────────────────
        c5 = _Card(self._main, title=_t("card_log"))
        c5.pack(fill="x", padx=24, pady=(6, 20))
        self._card_border(c5)
        self._reg(c5._title_label, "card_log")

        self.log_text = tk.Text(c5, height=6, bg=PANEL, fg=MUTED,
                                font=(FONT_MONO, 8), relief="flat",
                                wrap="word", state="disabled")
        self.log_text.pack(fill="x", padx=16, pady=(0, 12))

    def _card_border(self, widget):
        """Agrega un borde sutil a una tarjeta con canvas."""
        widget.configure(borderwidth=1, relief="solid")

    # ── Stdout → log widget ───────────────────────────────────────────────────

    def _redirigir_stdout(self):
        q = self._log_queue
        log_file = open(LOG_PATH, "a", encoding="utf-8")
        log_file.write(f"\n{'='*56}\n{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*56}\n")

        class _QStream:
            encoding = "utf-8"
            def __init__(self, orig):
                self._orig = orig
                self._lf   = log_file
            def write(self, data):
                if data:
                    q.put(data)
                    self._lf.write(data)
                    self._lf.flush()
                    try:    self._orig.write(data)
                    except: pass
            def flush(self): pass
            def reconfigure(self, **kw): pass

        sys.stdout = _QStream(sys.stdout)

    def _poll_log(self):
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self.log_text.configure(state="normal")
                self.log_text.insert("end", msg)
                self.log_text.see("end")
                self.log_text.configure(state="disabled")
        except q_module.Empty:
            pass
        self.after(100, self._poll_log)

    # ── Paso 1: Buscar ────────────────────────────────────────────────────────

    def _buscar(self):
        if not self.var_episodio.get().strip():
            messagebox.showwarning(_t("warn_missing") if _LANG == "es" else "Missing data",
                                   _t("warn_no_ep"))
            return
        self.btn_buscar.configure(state="disabled")
        self.lbl_buscar_st.configure(text=_t("searching"))
        self._limpiar_lista()

        def _run():
            recs = _obtener_recordings(self.api_key)
            orig = [r for r in recs if not _es_export(r)]
            orig.sort(key=lambda r: r.get("created_date") or r.get("created_at") or "9999",
                      reverse=True)
            self.after(0, lambda: self._on_buscar_done(orig))

        threading.Thread(target=_run, daemon=True).start()

    def _on_buscar_done(self, originales):
        self.btn_buscar.configure(state="normal")
        self.recordings = originales[:20]
        if not self.recordings:
            self.lbl_buscar_st.configure(text=_t("search_none"))
            return
        self.lbl_buscar_st.configure(
            text=_t("search_ok", total=len(originales), shown=len(self.recordings)))
        self._poblar_lista()

    def _limpiar_lista(self):
        for w in self.list_frame.winfo_children(): w.destroy()
        self.check_vars = []
        self.btn_dl.configure(state="disabled")

    def _poblar_lista(self):
        self._limpiar_lista()
        self.check_vars = []

        # Cabecera
        hdr = ttk.Frame(self.list_frame, style="Card.TFrame")
        hdr.pack(fill="x", padx=6, pady=(4, 2))
        ttk.Label(hdr, text=" ", style="Card.TLabel", width=3).pack(side="left")
        ttk.Label(hdr, text="Grabación", style="Muted.TLabel", width=44).pack(side="left")
        ttk.Label(hdr, text="Fecha", style="Muted.TLabel").pack(side="left", padx=8)

        sep = tk.Frame(self.list_frame, bg=BORDER, height=1)
        sep.pack(fill="x", padx=6, pady=(0, 4))

        for rec in self.recordings:
            var    = tk.BooleanVar(value=False)
            nombre = rec.get("name") or rec.get("title") or "Sin nombre"
            fecha  = (rec.get("created_date") or rec.get("created_at") or "—")[:10]

            row = ttk.Frame(self.list_frame, style="Card.TFrame")
            row.pack(fill="x", padx=6, pady=1)

            cb = tk.Checkbutton(row, variable=var, bg=PANEL,
                                activebackground=PANEL, selectcolor=PANEL,
                                relief="flat", bd=0)
            cb.pack(side="left")

            ttk.Label(row, text=nombre[:50], style="Card.TLabel", width=44,
                      anchor="w").pack(side="left")
            ttk.Label(row, text=fecha, style="Muted.TLabel").pack(
                side="left", padx=8)

            self.check_vars.append((rec, var))

        self.btn_dl.configure(state="normal")

    def _scroll_delta(self, event) -> int:
        """Normaliza el delta del scroll para Windows y macOS."""
        return -1 * event.delta if IS_MAC else -1 * (event.delta // 120)

    def _scroll_lista(self, event):
        self.list_canvas.yview_scroll(self._scroll_delta(event), "units")

    def _sel_todo(self):
        for _, v in self.check_vars: v.set(True)

    def _desel_todo(self):
        for _, v in self.check_vars: v.set(False)

    # ── Paso 2: Descargar ─────────────────────────────────────────────────────

    def _descargar(self):
        seleccionadas = [(rec, var) for rec, var in self.check_vars if var.get()]
        if not seleccionadas:
            messagebox.showwarning("Sin selección", "Seleccioná al menos una grabación.")
            return
        recs = [r for r, _ in seleccionadas]

        programa = self.var_programa.get()
        episodio = limpiar(self.var_episodio.get())
        self.carpeta_descarga = BASE_DIR / "riverside_downloads" / f"{programa}_{episodio}"
        self.carpeta_descarga.mkdir(parents=True, exist_ok=True)

        self.btn_dl.configure(state="disabled")
        self.btn_drive.configure(state="disabled")
        self.var_prog.set(0); self.var_prog2.set(0)
        self.lbl_dl_name.configure(text=_t("dl_starting"))

        total_recs  = len(recs)
        bajar_audio = self.var_audio.get()
        bajar_video = self.var_video.get()

        if not bajar_audio and not bajar_video:
            messagebox.showwarning(_t("warn_no_sel"), _t("warn_no_format"))
            self.btn_dl.configure(state="normal")
            return

        def _run():
            multiples = total_recs > 1
            archivos  = []

            for idx, rec in enumerate(recs, 1):
                nombre_gr = rec.get("name") or rec.get("title") or ""
                sufijo    = _sufijo_toma(nombre_gr, idx) if multiples else ""
                self._dl_queue.put(("overall", f"Recording {idx}/{total_recs}: {nombre_gr}",
                                    int((idx-1)/total_recs*100)))

                def _prog_cb(pct, fname,
                             _idx=idx, _total=total_recs, _nm=nombre_gr):
                    self._dl_queue.put(("file", fname, pct))
                    overall = int(((_idx-1) + pct/100) / _total * 100)
                    self._dl_queue.put(("overall_pct", overall))

                archivos_rec = _descargar_tracks(
                    rec, self.carpeta_descarga, self.api_key,
                    sufijo=sufijo, progress_cb=_prog_cb,
                    bajar_audio=bajar_audio, bajar_video=bajar_video)
                archivos.extend(archivos_rec)

            self._dl_queue.put(("done", archivos))

        def _poll():
            try:
                while True:
                    msg = self._dl_queue.get_nowait()
                    if msg[0] == "file":
                        self.lbl_dl_name.configure(text=msg[1])
                        self.var_prog.set(msg[2])
                    elif msg[0] == "overall":
                        self.lbl_dl_overall.configure(text=msg[1])
                        self.var_prog2.set(msg[2])
                    elif msg[0] == "overall_pct":
                        self.var_prog2.set(msg[1])
                    elif msg[0] == "done":
                        self.archivos_descargados = msg[1]
                        self.var_prog.set(100)
                        self.var_prog2.set(100)
                        self.lbl_dl_name.configure(
                            text=_t("dl_done", n=len(msg[1])))
                        self._construir_rename_ui()
                        return
            except q_module.Empty:
                pass
            self.after(150, _poll)

        threading.Thread(target=_run, daemon=True).start()
        self.after(150, _poll)

    # ── Paso 3: Rename UI ─────────────────────────────────────────────────────

    def _construir_rename_ui(self):
        for w in self.rename_wrap.winfo_children(): w.destroy()

        programa = self.var_programa.get()
        episodio = limpiar(self.var_episodio.get())

        self.rename_entries = []

        # Cabecera
        hdr = ttk.Frame(self.rename_wrap, style="Card.TFrame")
        hdr.pack(fill="x", pady=(4, 2))
        for txt, w in [("Archivo original", 28), ("Participante", 14),
                        ("Toma", 8), ("Nombre final", 30)]:
            ttk.Label(hdr, text=txt, style="Muted.TLabel", width=w,
                      anchor="w").pack(side="left", padx=(0, 4))

        tk.Frame(self.rename_wrap, bg=BORDER, height=1).pack(fill="x", pady=(2, 6))

        for info in self.archivos_descargados:
            archivo              = info["ruta"]
            part_auto, toma_auto = _parsear_nombre_descargado(archivo.name)

            row = ttk.Frame(self.rename_wrap, style="Card.TFrame")
            row.pack(fill="x", pady=3)

            ttk.Label(row, text=archivo.name[:28], style="Card.TLabel",
                      width=28, anchor="w").pack(side="left", padx=(0, 4))

            part_var    = tk.StringVar(value=part_auto)
            toma_var    = tk.StringVar(value=toma_auto)
            preview_var = tk.StringVar()

            def _upd(pv=part_var, tv=toma_var, prv=preview_var, ext=archivo.suffix):
                p = limpiar(pv.get()) or "Participante"
                t = tv.get().strip()
                prv.set(f"{programa}_{episodio}_{p}{'_'+t if t else ''}{ext}")

            part_var.trace_add("write", lambda *a, f=_upd: f())
            toma_var.trace_add("write", lambda *a, f=_upd: f())

            ttk.Entry(row, textvariable=part_var, width=14).pack(
                side="left", padx=(0, 4))
            ttk.Entry(row, textvariable=toma_var, width=8).pack(
                side="left", padx=(0, 8))
            _upd()
            ttk.Label(row, textvariable=preview_var, style="Green.TLabel",
                      width=34, anchor="w").pack(side="left")

            self.rename_entries.append((archivo, part_var, toma_var))

        self.btn_drive.configure(state="normal")

    # ── Configuración ────────────────────────────────────────────────────────

    def _abrir_config(self):
        _ConfigDialog(self, self.config_data)

    # ── Paso 4: Copiar a Drive ────────────────────────────────────────────────

    def _copiar_drive(self):
        programa = self.var_programa.get()
        episodio = limpiar(self.var_episodio.get())

        quiere_drive = self.var_dest_drive.get()
        quiere_local = self.var_dest_local.get()

        if not quiere_drive and not quiere_local:
            messagebox.showwarning("—", _t("warn_no_dest"))
            return

        # ── Destino Drive ──────────────────────────────────────────────────────
        drive_ok   = False
        raw_folder = None
        if quiere_drive:
            if DRIVE_BASE.exists():
                ep_folder = buscar_carpeta_episodio(programa, episodio)
                if ep_folder:
                    raw_folder = buscar_o_crear_carpeta_raw(ep_folder, programa, episodio)
                    drive_ok   = True
                else:
                    print(f"  ⚠  Drive: no se encontró carpeta con '#{episodio}'. Se omite Drive.")
            else:
                print(f"  ⚠  Drive no disponible ({DRIVE_BASE}). Se omite Drive.")

        # ── Destino local (DaVinci) ────────────────────────────────────────────
        local_folder = None
        local_ok     = False
        if quiere_local:
            local_folder = buscar_o_crear_carpeta_local(programa, episodio)
            local_ok     = local_folder is not None

        if not drive_ok and not local_ok:
            messagebox.showerror("Error", _t("err_no_dest"))
            return

        # ── Construir lista de nombres finales ────────────────────────────────
        plan = []   # [(archivo_src, nuevo_nombre), ...]
        for archivo, part_var, toma_var in self.rename_entries:
            p = limpiar(part_var.get()) or "Participante"
            t = toma_var.get().strip()
            nuevo_nombre = (
                f"{programa}_{episodio}_{p}_{t}{archivo.suffix}"
                if t else
                f"{programa}_{episodio}_{p}{archivo.suffix}"
            )
            plan.append((archivo, nuevo_nombre))

        # ── Chequear duplicados en Drive ──────────────────────────────────────
        drive_overwrite = True   # por defecto sobreescribir
        if drive_ok and raw_folder:
            existentes = [nm for _, nm in plan if (raw_folder / nm).exists()]
            if existentes:
                lista = "\n".join(f"  • {nm}" for nm in existentes)
                respuesta = messagebox.askyesnocancel(
                    _t("dup_title"),
                    _t("dup_msg", lista=lista),
                    icon="warning")
                if respuesta is None:       # Cancel
                    return
                drive_overwrite = respuesta # True = sobreescribir, False = saltear

        # ── Copiar archivos ────────────────────────────────────────────────────
        carpeta_final = self.carpeta_descarga / "listos"
        carpeta_final.mkdir(exist_ok=True)

        copiados_drive = 0
        copiados_local = 0
        salteados_drive = 0
        errores        = []

        for archivo, nuevo_nombre in plan:
            # Copiar a listos/ (copia renombrada temporal)
            nueva_ruta = carpeta_final / nuevo_nombre
            try:
                shutil.copy2(archivo, nueva_ruta)
            except Exception as e:
                errores.append(f"{nuevo_nombre} (rename): {e}")
                continue

            # → Google Drive (RAW)
            if drive_ok and raw_folder:
                dest_drive = raw_folder / nuevo_nombre
                ya_existe  = dest_drive.exists()
                if ya_existe and not drive_overwrite:
                    salteados_drive += 1
                    print(f"  ⏭  {nuevo_nombre}  →  Drive/RAW (ya existe, salteado)")
                else:
                    try:
                        shutil.copy2(nueva_ruta, dest_drive)
                        copiados_drive += 1
                        tag = " (sobreescrito)" if ya_existe else ""
                        print(f"  ✓ {nuevo_nombre}  →  Drive/RAW{tag}")
                    except Exception as e:
                        print(f"  ✗ Drive {nuevo_nombre}: {e}")
                        errores.append(f"{nuevo_nombre} (Drive): {e}")

            # → DaVinci local (Media)
            if local_ok and local_folder:
                try:
                    shutil.copy2(nueva_ruta, local_folder / nuevo_nombre)
                    copiados_local += 1
                    print(f"  ✓ {nuevo_nombre}  →  DaVinci/Media")
                except Exception as e:
                    print(f"  ✗ DaVinci {nuevo_nombre}: {e}")
                    errores.append(f"{nuevo_nombre} (DaVinci): {e}")

        # ── Resumen ────────────────────────────────────────────────────────────
        lineas = []
        if drive_ok:
            linea_drive = f"☁  Drive/RAW:      {copiados_drive} archivo(s)"
            if salteados_drive:
                linea_drive += f"  ({salteados_drive} ya existían, salteados)"
            lineas.append(linea_drive)
        if local_ok:   lineas.append(f"💻  DaVinci/Media:  {copiados_local} archivo(s)")
        resumen = "\n".join(lineas)

        if errores:
            messagebox.showwarning(_t("copy_errors_title"),
                f"{resumen}\n\n" + "\n".join(errores))
        else:
            messagebox.showinfo(_t("copy_done_title"), _t("copy_done", resumen=resumen))
            # Limpiar carpeta temporal de descarga
            try:
                shutil.rmtree(self.carpeta_descarga)
                print(f"  🗑  Carpeta temporal eliminada: {self.carpeta_descarga.name}")
            except Exception as e:
                print(f"  ⚠  No se pudo eliminar la carpeta temporal: {e}")


# ══════════════════════════════════════════════════════
#   ENTRYPOINT
# ══════════════════════════════════════════════════════

if __name__ == "__main__":
    if "--setup" in sys.argv:
        # Modo terminal: configurar studios
        _lf = _init_terminal_log()
        try:
            config = cargar_config()
            config = configurar_studios(config, forzar=True)
            print("  Listo. Volvé a correr el script sin --setup para la ventana.")
        finally:
            print(f"\nLog: {LOG_PATH.resolve()}")
            _lf.close()

    elif "--headless" in sys.argv:
        # Modo automático (sin GUI) — disparado por ark_watcher.py
        _lf = _init_terminal_log()
        try:
            args     = sys.argv[1:]
            show     = "CMB"
            episode  = ""
            title    = ""
            audio    = True
            video    = False
            drive    = True
            local    = True
            count    = 3
            for i, a in enumerate(args):
                if   a == "--show"    and i+1 < len(args): show    = args[i+1]
                elif a == "--episode" and i+1 < len(args): episode = args[i+1]
                elif a == "--title"   and i+1 < len(args): title   = args[i+1]
                elif a == "--count"   and i+1 < len(args):
                    try: count = int(args[i+1])
                    except ValueError: pass
                elif a == "--no-audio":  audio = False
                elif a == "--video":     video = True
                elif a == "--no-drive":  drive = False
                elif a == "--no-local":  local = False
            if not episode:
                print("  ✗ Falta --episode.  Uso: --headless --show CMB --episode 474")
                sys.exit(1)
            ok = _run_headless(show, episode,
                               title=title,
                               audio=audio, video=video,
                               drive=drive, local=local, count=count)
        finally:
            # Restaurar stdout antes de cerrar el archivo de log
            if isinstance(sys.stdout, Tee):
                sys.stdout = sys.stdout._stdout
            print(f"\nLog: {LOG_PATH.resolve()}")
            _lf.close()
        sys.exit(0 if ok else 1)

    else:
        # Modo GUI (por defecto)
        config = cargar_config()
        aplicar_config_carpetas(config)
        app    = ArkApp(config)
        app.mainloop()
