# DataChaos — byggeplan for Claude Code

**Repo:** DataChaos (tomt i dag) · **Python-pakke og CLI:** `datachaos` · **Python:** >= 3.10
**Søsterprosjekter:** dataowl (fakta om Databricks-tabeller) og DAGtracer / `dgt` (hvor en verdi
forsvinner i en dbt-DAG). Ingen felles kode med dem, men samme konvensjoner (se § 3).

Dette dokumentet erstatter ChatGPT-versjonen. Produktidéen er den samme; endringene gjelder
datamodell, determinisme, verifisering av deteksjon og rekkefølgen arbeidet gjøres i.
§ 16 oppsummerer hva som er endret og hvorfor.

---

## 1. Produkt i én setning

DataChaos lager et syntetisk, deterministisk datasett levert i **batcher**, injiserer
kontrollerte datakvalitetsfeil, fører en **ledger** over nøyaktig hva som ble endret, og
sjekker om pipelinen (dbt-testene) faktisk oppdaget feilene.

Kjerneløkken:

```
generate  →  inject (scenarioer)  →  land batch for batch  →  dbt build  →  check-dbt
                     │                                                         │
                     └──────────────── chaos-ledger.jsonl ─────────────────────┘
                           (hva vi ødela)                 (hva pipelinen fanget)
```

Uten det siste steget er DataChaos bare en testdatagenerator. `check-dbt` er det som gjør
verktøyet verdt å bruke, så det er med i MVP.

## 2. Ikke-mål (gjelder hele MVP)

- Ingen Databricks-, Spark- eller warehouse-skriving. Kun lokale filer.
- Ingen dashboard, web-UI, server eller SaaS.
- Ingen LLM/AI, ingen nettverkskall under kjøring.
- Ingen vilkårlig Python i konfigurasjon (ingen `eval`, ingen plugin-lasting fra config).
- Ingen pandas, PyArrow eller DuckDB som runtime-avhengighet. Eneste runtime-avhengighet er
  `pyyaml` (samme som `dgt`).
- Ingen injeksjon i brukerens egne vilkårlige CSV-filer. MVP opererer bare på datasett
  DataChaos selv har generert (de har kjent schema). Se § 15, punkt 2.
- Ingen «fuzzing» uten seed og ledger.

## 3. Konvensjoner felles med dataowl og dgt

| Område | Regel |
|---|---|
| Pakking | `src/`-layout, hatchling (som dataowl), MIT-lisens, `py.typed` |
| Kvalitet | ruff (samme regelsett som dataowl: `E,F,I,UP,B,SIM`, linjelengde 100, `from __future__ import annotations` påkrevd), mypy strict-ish (kopier `[tool.mypy]` fra dataowl), pytest |
| CI | GitHub Actions: Ubuntu med Python 3.10–3.13, pluss macOS og Windows med Python 3.12. `ruff check`, `ruff format --check`, `mypy`, `pytest` |
| Filer | Tekstfiler åpnes alltid med `encoding="utf-8", newline=""` (testes). Binærmodus brukes bare til å hashe filer, slik at en ødelagt fil gir feil hash og ikke dekodingsfeil |
| Maskinlesbar output | Hvert JSON-dokument har `schema_version` og `tool_version`. Minor-bump legger bare til felter |
| Exit codes | `0` OK · `1` kjøringen fullførte, men validering/forventning feilet · `2` bruks-, konfig-, IO- eller verktøyfeil |
| Stdout/stderr | Resultat til stdout, status og advarsler til stderr. Med `--output json` er stdout kun JSON |
| Feilmeldinger | Én linje med `datachaos: error: …`, ingen stacktrace uten `--verbose` |
| Dokumenter | `CHANGELOG.md` (Keep a Changelog), README med eksempler som er ekte output |

## 4. Arkitektur

```
src/datachaos/
  __init__.py          # __version__, offentlig API: generate(), run(), validate()
  __main__.py
  cli.py               # argparse, ingen forretningslogikk
  errors.py            # DataChaosError (exit 2), ValidationFailed (exit 1)
  rng.py               # deterministisk RNG, § 6
  timeutil.py          # parsing av "1h"/"2d", ISO-formattering, UTC overalt
  model/
    schema.py          # ColumnType, Column, Role, TableSchema
    table.py           # Table, Batch – immutable
    dataset.py         # Dataset = navngitt samling tabeller + batch-oppsett
    ledger.py          # LedgerEntry
    manifest.py        # Manifest-dataklasser + to_dict()
  datasets/
    __init__.py        # registry: {"shop": build_shop}
    shop.py            # customers, orders, order_events
  scenarios/
    __init__.py        # registry, oppslag på navn
    base.py            # Scenario-protokoll, ScenarioSpec, ParamSpec, select_rows()
    duplicate_business_key.py
    null_critical_column.py
    invalid_enum.py
    orphaned_foreign_key.py
    late_arriving_data.py
    out_of_order_events.py
    historical_update.py
    schema_drift.py
  config.py            # YAML → RunConfig, streng validering
  engine.py            # kjører generator + scenarioer i rekkefølge, bygger ledger
  io/
    csv_writer.py
    jsonl_writer.py
    reader.py          # leser DataChaos-output tilbake (for validate/inject)
    output_dir.py      # trygg skriving: temp-mappe + atomisk rename
  validate.py          # sjekker filer mot manifest og ledger
  dbtcheck.py          # leser dbt run_results.json, sammenligner med forventninger
  land.py              # kopierer batch 0..k til en landing-mappe
tests/
  unit/  integration/  golden/
examples/
  dbt_shop/            # dbt-duckdb-prosjekt, § 14, fase 6
docs/
  scenarios.md         # generert fra scenario-metadata, § 12
  output-format.md
schema/
  chaos-manifest-1.0.schema.json
```

Lagdeling (håndheves med en test som parser imports, slik dataowl gjør i
`test_import_boundaries.py`):

- `model/` importerer ingenting fra resten av pakken.
- `datasets/` og `scenarios/` importerer bare `model/`, `rng`, `timeutil`, `errors`.
  **Scenarioer gjør aldri IO.**
- `io/` kjenner ikke scenarioer.
- `engine.py` binder sammen; `cli.py` kaller bare `engine`, `validate`, `dbtcheck`, `land`.

## 5. Datamodell

### 5.1 Typer og roller

```python
class ColumnType(Enum):  STRING, INTEGER, DECIMAL, BOOLEAN, DATE, TIMESTAMP
class Role(Enum):        KEY, FOREIGN_KEY, EVENT_TIME, INGESTION_TIME, UPDATED_AT,
                         ENUM, ATTRIBUTE

@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    nullable: bool
    role: Role = Role.ATTRIBUTE
    references: str | None = None        # "customers.customer_id" for FOREIGN_KEY
    allowed_values: tuple[str, ...] = () # for ENUM
    scale: int | None = None             # for DECIMAL

@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: tuple[Column, ...]
    kind: Literal["entity", "event_log"]
```

Roller lar scenarioer finne fornuftige standardparametere (f.eks. nøkkelkolonne), men den
**oppløste** parameterverdien skrives alltid i manifestet. Ingenting er implisitt i output.

### 5.2 Batcher

Data leveres i batcher, slik en ekte landing zone får dem:

- Konfig: `batches` (antall, standard 1) og `batch_interval` (standard `1d`), og `as_of`
  (slutten av vinduet, påkrevd i config, standard `2026-01-01T00:00:00Z` på CLI).
- Vindu: `start = as_of - batches * batch_interval`. Batch `k` inneholder radene der
  `ingested_at ∈ [start + k·interval, start + (k+1)·interval)`.
- **Batch-tilhørighet utledes alltid fra `ingested_at`.** Scenarioer som endrer
  `ingested_at` flytter dermed rader mellom batcher automatisk.
- Et scenario som ville skyve `ingested_at` forbi `as_of` er en **valideringsfeil** ved
  oppstart (meldingen sier hvilken parameter som er for stor), ikke stille klipping.
- Hver batch har sitt eget schema, slik at schema-drift kan starte i en gitt batch.

`batches: 1` gir en vanlig «én fil per tabell»-snapshot, som ChatGPT-planen beskrev.

### 5.3 Tabell-representasjon

```python
@dataclass(frozen=True)
class Table:
    schema_by_batch: tuple[TableSchema, ...]   # én per batch
    rows: tuple[Row, ...]                      # alle batcher, stabilt sortert

@dataclass(frozen=True)
class Row:
    rid: str               # intern rad-ID, aldri skrevet til data (se --emit-row-ids)
    values: Mapping[str, Value]
```

- `rid` gis ved generering (`c000001`, `o000001`, `e0000001`) og for nye rader fra scenario
  `i`: `s{i:04d}-{n:07d}` (`s0000-0000001`). Fast bredde gjør at sortering på `rid` er numerisk. Den brukes i ledgeren og til eksklusivitet (§ 7.3).
- Immutable: scenarioer returnerer ny `Table`. En test sjekker at input-`Dataset` er uendret
  (sammenlign `hash` av en dyp kopi før og etter).
- Rekkefølge i output: sortert på `(batch, ingestion_time, rid)`. Fysisk radrekkefølge har
  ingen semantikk i SQL, så ingen scenario baserer seg på den (se § 8.6).

### 5.4 Innebygd datasett `shop`

Ett datasett med tre tabeller som henger sammen, ikke separate `--dataset orders`-kall.
(Genereres `orders` alene kan ikke `orphaned-foreign-key` gi mening.)

**customers** (`entity`)

| Kolonne | Type | Rolle | Merknad |
|---|---|---|---|
| customer_id | STRING | KEY | `C000001` … |
| created_at | TIMESTAMP | EVENT_TIME | |
| updated_at | TIMESTAMP | UPDATED_AT | = created_at i baseline |
| country_code | STRING | ENUM | NO, SE, DK, FI, DE |
| customer_segment | STRING | ENUM | consumer, smb, enterprise |
| is_active | BOOLEAN | ATTRIBUTE | |
| ingested_at | TIMESTAMP | INGESTION_TIME | created_at + 0–30 min |

**orders** (`entity`)

| Kolonne | Type | Rolle | Merknad |
|---|---|---|---|
| order_id | STRING | KEY | `O000001` … |
| customer_id | STRING | FOREIGN_KEY → customers.customer_id | velges blant kunder med `created_at ≤ order_timestamp` |
| order_timestamp | TIMESTAMP | EVENT_TIME | jevnt fordelt over vinduet |
| updated_at | TIMESTAMP | UPDATED_AT | = order_timestamp i baseline |
| status | STRING | ENUM | alltid `created` i baseline (tilstanden ved opprettelse); øvrige verdier: paid, shipped, delivered, cancelled |
| amount | DECIMAL(18,2) | ATTRIBUTE | genereres som heltall øre/cent |
| currency | STRING | ENUM | NOK, SEK, DKK, EUR |
| source_system | STRING | ENUM | webshop, pos, b2b_api |
| ingested_at | TIMESTAMP | INGESTION_TIME | order_timestamp + 0–30 min |

**order_events** (`event_log`) — trengs for at late-arriving og out-of-order skal bety noe

| Kolonne | Type | Rolle | Merknad |
|---|---|---|---|
| event_id | STRING | KEY | `E0000001` … (7 sifre: opptil 5 events for hver av 899 999 ordrer) |
| order_id | STRING | FOREIGN_KEY → orders.order_id | |
| event_type | STRING | ENUM | created, paid, shipped, delivered, cancelled |
| event_time | TIMESTAMP | EVENT_TIME | strengt økende per ordre; første event er `created` med `event_time = order_timestamp` |
| ingested_at | TIMESTAMP | INGESTION_TIME | event_time + 0–30 min |

Generatorkrav:

- Størrelser i config: `customers`, `orders` (events følger av ordrene, 1–5 per ordre).
  CLI-standard: 100 kunder, 1 000 ordrer.
- Baseline er **ren**: alle `unique`, `not_null`, `relationships` og `accepted_values` som
  er naturlige for schemaet holder. Dette testes (§ 13) og er kontrollgruppen i `check-dbt`.
- Ingen tom streng genereres noensinne (tom felt = NULL i CSV, § 9.1).
- Ingen ekte persondata, ingen nettverk, ingen systemtid. Alle tider avledes fra `as_of`.
- Alle baseline-rader har `ingested_at < as_of` per konstruksjon.
- **Ingen rad inneholder informasjon fra etter sin egen `ingested_at`** (testes). `orders` er
  derfor tilstanden ved opprettelse: `status = created`, `updated_at = order_timestamp`. Senere
  statusendringer finnes bare i `order_events`.
- Forløp i `order_events`, der hver ordre trekker ett:
  - `created → paid`
  - `created → paid → shipped`
  - `created → paid → shipped → delivered`
  - `created → cancelled`
  - `created → paid → cancelled`
  - `created → paid → shipped → delivered → cancelled`
- Events som ville fått `ingested_at` på eller etter `as_of` kuttes bort, sammen med resten av
  forløpet. Antall events per ordre er derfor 1–5, og første event er alltid med.
- Første kunde (`C000001`) opprettes ved vinduets start, så det alltid finnes en kunde å velge.
  Sent opprettede kunder kan stå uten ordrer.
- En forelder lander aldri etter barnet: `ingested_at` for kunde ≤ ordre ≤ ordrens events.
- Avstanden mellom events i en ordre er 31 min–6 t, så `ingested_at` har samme rekkefølge som
  `event_time` i baseline. `out-of-order-events` har dermed en ren kontrollgruppe.
- `batches × batch_interval` må være minst 3 t. `customers` og `orders` er maks 899 999, fordi
  `C9xxxxx` og `O9xxxxx` er reservert for `orphaned-foreign-key`. `batches` er maks 9 999.

## 6. Determinisme

Kontrakt: samme DataChaos-versjon + samme config (inkl. seed) ⇒ byte-identiske datafiler,
ledger og manifest bortsett fra `manifest.run.created_at`. Dette skal holde på tvers av
Python 3.10–3.13 og operativsystemer.

Hvorfor ChatGPT-oppskriften ikke holder: Python garanterer kun at `Random.random()` gir samme
sekvens på tvers av versjoner. `randrange`, `choice`, `sample` og `shuffle` er ikke garantert
og har endret seg mellom versjoner. Derfor:

```python
# rng.py
def derive_seed(global_seed: int, purpose: str) -> int:
    digest = hashlib.sha256(f"datachaos:{global_seed}:{purpose}".encode()).digest()
    return int.from_bytes(digest[:8], "big")

class Rng:
    """Bygger kun på random.Random.random(), som er stabil på tvers av Python-versjoner."""
    def __init__(self, seed: int) -> None: self._r = random.Random(seed)
    def random(self) -> float: return self._r.random()
    def below(self, n: int) -> int: ...          # int(random() * n), n > 0
    def between(self, lo: int, hi: int) -> int: ...  # inklusiv
    def choice(self, seq: Sequence[T]) -> T: ...
    def weighted_choice(self, items: Sequence[T], weights: Sequence[int]) -> T: ...
    def sample_indices(self, n: int, k: int) -> list[int]: ...  # partiell Fisher–Yates, sortert retur
    def shuffle(self, items: list[T]) -> None: ...  # Fisher–Yates med below()
```

Regler:

- Ingen kall til `random`-modulens globale funksjoner noe sted. En test grepper kildekoden.
- Ingen `hash()` (randomisert per prosess), ingen iterasjon over `set` uten `sorted()`.
- Seed per formål: `dataset:customers`, `dataset:orders`, `dataset:order_events`,
  `scenario:{index}:{name}`. Å legge til et scenario på slutten endrer ikke data fra
  tidligere scenarioer.
- Beløp genereres som heltall (øre) og formatteres som streng. Ingen flyttall i output.
- `run_id` = første 16 hex-tegn av SHA-256 over den kanoniske, oppløste configen + versjon.
- Golden tests (§ 13.3) kjøres i hele CI-matrisen; det er der kryss-versjonsgarantien testes.

## 7. Scenario-rammeverk

### 7.1 Protokoll

```python
@dataclass(frozen=True)
class ParamSpec:
    name: str
    type: type | tuple[type, ...]
    required: bool
    default: object = None
    help: str = ""
    validate: Callable[[object, TableSchema], str | None] | None = None  # feilmelding eller None

@dataclass(frozen=True)
class ScenarioSpec:
    name: str                       # "duplicate-business-key"
    version: int                    # økes når utvalg eller transformasjon endres
    summary: str                    # én linje
    detects: str                    # hvilken pipelinefeil scenarioet avslører
    typical_dbt_test: str           # "unique", "not_null", ...
    applies_to: frozenset[str]      # {"entity"}, {"event_log"} eller begge
    params: tuple[ParamSpec, ...]

class Scenario(Protocol):
    spec: ScenarioSpec
    def resolve(self, ctx: ResolveContext, table: str, params: Mapping[str, object]) -> ResolvedParams: ...
    def schemas_after(self, ctx: ResolveContext, table: str, params: ResolvedParams) -> SchemaState: ...
    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome: ...
    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]: ...

@dataclass(frozen=True)
class ResolveContext:
    dataset: Dataset      # baseline
    schemas: SchemaState  # {tabell: schema per batch} etter alle tidligere scenarioer

@dataclass(frozen=True)
class ScenarioContext:
    dataset: Dataset
    table: str
    params: ResolvedParams
    rng: Rng
    index: int
    eligible: Callable[[Row], bool]   # eksklusivitetsfilter fra engine, § 7.3

@dataclass(frozen=True)
class ScenarioOutcome:
    dataset: Dataset
    ledger: tuple[LedgerEntry, ...]
    stats: ScenarioStats   # rows_before, rows_after, rows_selected, rows_added, values_changed
```

- `resolve` fyller inn standardverdier fra roller og validerer alle parametere **før** noe
  scenario kjører. Ukjente parametere er feil (med «did you mean» via `difflib`). Den er
  idempotent, slik at den oppløste configen i manifestet gir samme kjøring.
- Engine resolver scenarioene i rekkefølge og legger `schemas_after` fra hvert scenario inn i
  `ResolveContext` til det neste. Kolonnenavn og typer valideres mot `ctx.schemas`, så et senere
  scenario ser endringene `schema-drift` gjør (fase 4). Standard er uendret schema.
- `check` er scenarioets invariant (se hvert scenario). Kjøres rett etter `apply`, med tilstanden
  før i `ctx.dataset` og etter i `outcome.dataset`. Ikke-tom liste ⇒ intern feil (exit 2), fordi
  det betyr en bug i DataChaos. Invarianter sjekkes relativt til tilstanden før scenarioet.
- `rate`, `count` og `allow_overlap` ligger under `params` i configen, som alle andre parametere.

### 7.2 Utvalg av rader

Felles funksjon i `base.py`, brukt av alle scenarioer:

```python
def select_rows(rows, rng, *, rate=None, count=None, eligible) -> list[Row]
```

- Nøyaktig én av `rate` (0 < rate ≤ 1) og `count` (≥ 1). Begge eller ingen ⇒ feil.
- `n = floor(rate * len(kandidater) + 0.5)`. Hvis `rate > 0` gir `n == 0`: feil med melding
  som foreslår `count`.
- `count > len(kandidater)` ⇒ feil som oppgir antall kandidater.
- Kandidater sorteres på `rid` før trekking; utvalget returneres sortert på `rid`.

### 7.3 Flere scenarioer i én kjøring

Scenarioer kjøres i config-rekkefølge. Uten regler kan et senere scenario overskrive et
tidligere (null-setter nøkkelen som nettopp ble duplisert), og ledgeren blir feil.

- **Standard: eksklusivitet per forretningsnøkkel og tabell.** En rad hvis `KEY`-verdi er
  berørt av et tidligere scenario er ikke kandidat. Engine bygger `eligible`-filteret.
- `allow_overlap: true` på scenarioet slår det av. Ledgeren blir fortsatt korrekt fordi den
  skriver før-/etterverdi per endring, men `validate` kan da bare sjekke siste tilstand.
- Kolonne-schema-drift (`drop_column`, `rename_column`) på en kolonne som et tidligere
  scenario har endret er en valideringsfeil ved oppstart.

### 7.4 Ledger

`chaos-ledger.jsonl`, én linje per endring, sortert på `(scenario_index, table, rid, column)`:

```json
{"scenario_index":0,"scenario":"duplicate-business-key","table":"orders","action":"insert_row","rid":"s0000-0000001","source_rid":"o000417","key":{"order_id":"O000417"},"batch":3,"column":null,"before":null,"after":null}
{"scenario_index":1,"scenario":"null-critical-column","table":"orders","action":"set_value","rid":"o000052","key":{"order_id":"O000052"},"batch":0,"column":"customer_id","before":"C000031","after":null}
```

`action` ∈ `insert_row`, `delete_row`, `set_value`, `move_batch`, `schema_change`.
Ledgeren er grunnlaget for `validate`, for `report`, og for å gi brukeren konkrete verdier å
spore med `dgt` (§ 15, punkt 4).

`--emit-row-ids` legger til kolonnen `_chaos_rid` i alle datafiler. Av som standard, fordi
den endrer schemaet pipelinen ser.

## 8. Scenarioer i MVP

Åtte scenarioer, valgt fordi hvert enkelt treffer en vanlig dbt-test eller en vanlig feil i
inkrementelle modeller. Alle har `rate`/`count` og `allow_overlap` i tillegg til det som står.

### 8.1 `duplicate-business-key`

- **Avslører:** grain-brudd; staging uten dedup. **dbt-test:** `unique`.
- **Gjelder:** `entity`.
- **Parametere:** `key_columns` (standard: kolonner med rolle KEY), `mutation_mode`
  (`exact_copy` | `mutate_non_key`, standard `exact_copy`), `same_batch` (bool, standard
  `true`; `false` legger kopien i en senere batch).
- **Transformasjon:** for hver valgt rad, legg til en ny rad med samme nøkkel. Med
  `mutate_non_key` endres én ikke-nøkkel-, ikke-tidskolonne til en annen gyldig verdi.
  `ingested_at` for kopien: som originalen +1 s, men uendret hvis +1 s ville krysset batchgrensen
  eller `as_of`. Med `same_batch: false`: i en tilfeldig senere batch, og rader i siste batch er
  ikke kandidater (`batches: 1` er valideringsfeil). `mutate_non_key` velger blant ENUM- og
  ATTRIBUTE-kolonner, ikke FK, og ledgeren viser `column`, `before` og `after` på `insert_row`.
- **`key_columns`:** må identifisere radene unikt i baseline, ellers feil ved oppstart.
- **Invariant:** hver valgt nøkkel forekommer nøyaktig 2 ganger (med `allow_overlap`: antallet før
  pluss antall kopier); ingen originalrad endret; `rows_after == rows_before + n`.

### 8.2 `null-critical-column`

- **Avslører:** manglende `not_null`, joins som mister rader på NULL. **dbt-test:** `not_null`.
- **Parametere:** `column` (påkrevd). Rolle KEY er avvist med mindre `allow_key: true`. Rolle
  INGESTION_TIME er alltid avvist, fordi batch utledes fra den. Rader som allerede er NULL er ikke
  kandidater. Ledgerens `key` viser den opprinnelige verdien.
- **Transformasjon:** sett verdien til NULL. Kolonnen trenger ikke være nullable i schemaet;
  poenget er nettopp å bryte det. Manifestet registrerer at kontrakten er brutt, i `warnings`.
- **Invariant:** antall NULL i kolonnen øker med `n`; alle andre verdier på berørte rader uendret.

### 8.3 `invalid-enum`

- **Avslører:** ukjente kategorier som faller ut i `CASE`/mapping. **dbt-test:** `accepted_values`.
- **Parametere:** `column` (må ha rolle ENUM), `values` (liste, standard `["UNKNOWN"]`),
  `variant` (`literal` | `case_change` | `whitespace`; `case_change` gir `PAID`, og `no` for en verdi
  som allerede er store bokstaver; `whitespace` gir `"paid "`). `values` er bare lov med `literal`,
  og kan ikke inneholde tom streng (NULL i CSV) eller gyldige verdier. Bare rader med gyldig verdi
  er kandidater.
- **Invariant:** berørte verdier ∉ `allowed_values`; ingen andre kolonner endret.

### 8.4 `orphaned-foreign-key`

- **Avslører:** inner join som mister rader, manglende referanseintegritet.
  **dbt-test:** `relationships`.
- **Parametere:** `column` (standard: første FOREIGN_KEY), `mode` (`nonexistent` |
  `deleted_parent`). `nonexistent` setter verdien til en ID som garantert ikke finnes
  (`C9xxxxx`-serie). `deleted_parent` fjerner den refererte forelderen fra foreldertabellen
  (det eneste scenarioet som fjerner rader; ledger `action: "delete_row"`).
- **Utvalg ved `deleted_parent`:** `rate`/`count` velger foreldre (kunder), ikke barn. Alle
  barnerader som peker på en slettet forelder blir foreldreløse og føres i ledgeren som
  berørte (de regnes også som brukt i eksklusivitetsregelen, § 7.3).
- **Invariant:** hver berørt verdi finnes ikke i den refererte kolonnen (anti-join tom
  for treff); ved `nonexistent` er foreldertabellen uendret.

### 8.5 `late-arriving-data`

- **Avslører:** inkrementelle modeller med vannmerke på event time, som aldri plukker opp
  sene rader. **dbt-test:** egen singular test som sammenligner inkrementell og full
  refresh (§ 14, fase 6).
- **Gjelder:** begge; mest meningsfull på `order_events`.
- **Parametere:** `delay_min`, `delay_max` (varighet, f.eks. `"6h"`, `"3d"`). Valideres mot
  `as_of` for alle kandidatrader ved oppstart.
- **Transformasjon:** `ingested_at += delay` (jevnt fordelt i intervallet, hele sekunder).
  Event time røres ikke. Batch følger av ny `ingested_at`.
- **Invariant:** event time uendret; `delay_min ≤ ny − gammel ≤ delay_max`; ledger har
  `set_value` på `ingested_at` og `move_batch` når batch endres.

### 8.6 `out-of-order-events`

- **Avslører:** «siste event vinner»-logikk som sorterer på `ingested_at` i stedet for
  `event_time`, eller som antar at events kommer i rekkefølge. **dbt-test:** singular test på
  at nåværende status = status fra siste `event_time`.
- **Gjelder:** `event_log`.
- **Parametere:** `group_by` (standard: FOREIGN_KEY-kolonnen, dvs. `order_id`).
- **Transformasjon:** for hver valgt gruppe med ≥ 2 events, bytt `ingested_at` mellom siste
  og nest siste event (etter event_time). Event time røres ikke. Dette er definisjonen; ikke
  «stokk radene i fila», som ikke har noen betydning i SQL.
- **Invariant:** i hver berørt gruppe er rekkefølgen på `ingested_at` ulik rekkefølgen på
  `event_time`; antall rader uendret.

### 8.7 `historical-update`

- **Avslører:** inkrementelle modeller som bare ser nye nøkler, eller som filtrerer på
  `order_timestamp` i stedet for `updated_at`.
- **Gjelder:** `entity`.
- **Parametere:** `older_than` (varighet relativt til `as_of`, f.eks. `"3d"`),
  `columns` (standard `["status", "amount"]`), `in_batch` (standard: siste batch).
- **Transformasjon:** for valgte rader med event time < `as_of − older_than`: legg til en
  **ny versjon** av raden i `in_batch` med endrede verdier, ny `updated_at` og ny
  `ingested_at` innenfor batchen. Originalraden beholdes, slik en kilde med inkrementell
  uttrekking oppfører seg. (ChatGPT-versjonen endret raden på stedet; det tester ikke det
  pipelinen faktisk møter.)
- **Invariant:** nøkkel uendret; ny `updated_at` > gammel; event time uendret;
  `rows_after == rows_before + n`.
- **Merk:** gir to rader per nøkkel i rådata. Det er legitimt, og `stg_orders` i eksemplet
  dedupliserer på siste `updated_at`. `duplicate-business-key` skiller seg ved at kopien har
  samme `updated_at`.

### 8.8 `schema-drift`

- **Avslører:** pipelines som feiler eller stille mister data ved schemaendring.
- **Parametere:** `operation` (`add_column` | `drop_column` | `rename_column` |
  `change_type`), `column`, `from_batch` (standard: siste batch), samt `new_name`
  (rename) og `to` (change_type).
- **Støttede `change_type`-konverteringer** (fast liste, ikke generell):

  | Fra | `to` | Eksempel |
  |---|---|---|
  | DECIMAL | `string_comma_decimal` | `12.50` → `12,50` |
  | TIMESTAMP | `string_local_format` | `2026-01-05T14:05:00Z` → `05.01.2026 14:05` |
  | INTEGER/DECIMAL | `string` | samme tekst, men schema sier STRING |
  | BOOLEAN | `string_yes_no` | `true` → `yes` |

- **Transformasjon:** gjelder batch ≥ `from_batch`. Endrer `schema_by_batch` og verdiene.
  `add_column` fyller med en deterministisk generert verdi eller NULL (`fill: null|generated`).
- **Invariant:** batcher før `from_batch` er byte-identiske med før scenarioet; schema-filene
  for batcher fra `from_batch` reflekterer endringen.
- Kan ikke kombineres med andre scenarioer på samme kolonne i samme kjøring (§ 7.3).

## 9. Output

### 9.1 Filformat

```
<out>/
  chaos-manifest.json
  chaos-ledger.jsonl
  data/<table>/batch_0000.csv        # og/eller .jsonl
  schemas/<table>/batch_0000.json
  data/<table>.csv                   # kun med --combined og når alle batcher har samme schema
```

Hver batch får alltid sine filer. En batch uten rader gir en CSV med bare header og en tom
JSONL-fil. `chaos-ledger.jsonl` finnes alltid, og er tom når ingen scenarioer har kjørt.

CSV, fast definert (dette er en del av determinisme-kontrakten):

- UTF-8 uten BOM, `\n`, header, `csv.writer(quoting=QUOTE_MINIMAL, lineterminator="\n")`.
- **NULL = tomt felt.** Generatoren lager aldri tom streng, så det er entydig. Dokumenter
  dette i README (DuckDB og dbt seeds leser tomt felt som NULL).
- DECIMAL: streng med fast antall desimaler. BOOLEAN: `true`/`false`.
  TIMESTAMP: `YYYY-MM-DDTHH:MM:SSZ`. DATE: `YYYY-MM-DD`.
- Etter `schema-drift` følger verdiene det nye schemaet for den batchen.

JSONL: ett objekt per linje, nøkler i schema-rekkefølge,
`json.dumps(obj, ensure_ascii=False, separators=(",", ":"))`, NULL som `null`, DECIMAL og
tider som strenger.

Schema-fil per batch:
`{"schema_version":"1.0","tool_version":…,"table":…,"kind":…,"batch":…,"columns":[{name,type,nullable,role,…}]}`.
`kind` trengs for å lese schemaet tilbake. Kolonnefeltene `references`, `allowed_values` og `scale`
skrives bare når de er satt.

Med `--emit-row-ids` står `_chaos_rid` som siste kolonne i både datafilene og schema-filene, slik
at de alltid stemmer overens.

### 9.2 Manifest (`chaos-manifest.json`, schema 1.0)

```jsonc
{
  "schema_version": "1.0",
  "tool_version": "0.1.0",
  "run": {
    "run_id": "3f9c0a7d1b2e4c55",
    "created_at": "2026-11-06T10:15:00Z",   // eneste ikke-deterministiske felt
    "seed": 42,
    "config": { /* fullt oppløst config, inkl. standardverdier */ }
  },
  "dataset": {
    "name": "shop", "generator_version": 1, "as_of": "2026-01-01T00:00:00Z",
    "batches": 7, "batch_interval": "1d",
    "tables": {"orders": {"rows_baseline": 1000, "rows_final": 1037}, "...": {}}
  },
  "scenarios": [
    {"index": 0, "name": "duplicate-business-key", "version": 1, "table": "orders",
     "seed": 1234567890123, "params": {...}, "stats": {...}, "ledger_entries": 20}
  ],
  "files": [{"path": "data/orders/batch_0000.csv", "sha256": "...", "rows": 143}],
  "content_sha256": "...",       // over ledger + alle datafiler i sortert rekkefølge
  "warnings": []
}
```

JSON Schema i `schema/` (2020-12), og testene validerer hvert manifest mot det (samme
mønster som `dgt`). `jsonschema` er dev-avhengighet.

### 9.3 Trygg skriving

- Output-mappen må ikke finnes, eller være tom. `--force` er bare lov hvis mappen inneholder
  et `chaos-manifest.json` fra før og alle filene i mappen står oppført i det (hindrer at noen
  sletter en vilkårlig mappe eller egne filer). Den gamle mappen fjernes først når det nye
  resultatet er ferdig skrevet.
- Manglende foreldermapper opprettes (`mkdir -p`).
- Skriv til `<out>.tmp-<run_id>` i samme foreldermappe, rename til slutt. Ved feil fjernes
  temp-mappen. Ingenting skrives utenfor output-mappen, bortsett fra temp-mappen og
  foreldermappene over.
- En temp-mappe fra en tidligere kjøring gir exit 2 og slettes ikke. Vi sletter bare det denne
  kjøringen selv har opprettet.
- `inject` (§ 10) leser fra én mappe og skriver alltid til en ny.

## 10. CLI

```
datachaos --version
datachaos list-scenarios [--output text|json]
datachaos describe <scenario> [--output text|json]
datachaos generate --out DIR [--seed N] [--customers N] [--orders N] [--batches N]
                   [--batch-interval 1d] [--as-of TS] [--format csv|jsonl|both] [--combined]
datachaos run --config chaos.yml --out DIR [--force]
datachaos run --out DIR --scenario NAME --table T [--param key=value ...] [--seed N] ...
datachaos inject --input DIR --out DIR --scenario NAME --table T [--param key=value ...]
datachaos validate DIR [--reproduce] [--output text|json]
datachaos report DIR [--output text|json]
datachaos land DIR --upto-batch K --to LANDING_DIR
datachaos check-dbt --run DIR --run-results PATH --config chaos.yml [--output text|json]
```

- `--out` er alltid påkrevd, også med `--config`. Stien er ikke en del av configen, så `run_id`
  er uavhengig av hvor resultatet skrives.
- Flere scenarioer i én kjøring går via config-fila. CLI-varianten av `run` tar ett scenario.
  (ChatGPT-planen viste `--scenario` gjentatt uten måte å gi parametere per scenario.)
- `--param key=value`: verdi parses som YAML-skalar (`0.02`, `true`, `[a,b]`, `"3d"`).
- `inject` leser en DataChaos-output-mappe (bruker manifest og schema-filer), legger til
  scenarioet sist og skriver et nytt manifest der det forrige er innebygd.
- `validate`: sjekker SHA-256 for alle filer, at hver ledger-linje er observerbar i data
  (nøkkel finnes i angitt batch, verdi = `after`), og scenarioenes invarianter på data lest
  fra disk. `--reproduce` kjører configen fra manifestet på nytt i en temp-mappe og krever
  lik `content_sha256`. Exit 1 ved avvik.
- `report`: menneskelesbar oppsummering per scenario med antall og 5 eksempel-nøkler.
- `land`: kopierer `batch_0000 … batch_K` for alle tabeller til en landing-mappe
  (`LANDING/<table>/batch_XXXX.csv`). Brukes av dbt-eksemplet for å simulere inkrementell
  ankomst.

## 11. Konfig og `check-dbt`

### 11.1 Konfigfil

```yaml
version: 1
seed: 42
dataset:
  name: shop
  as_of: "2026-01-08T00:00:00Z"
  customers: 200
  orders: 2000
  batches: 7
  batch_interval: 1d
output:
  formats: [csv]
  combined: false
  emit_row_ids: false
scenarios:
  - name: duplicate-business-key
    table: orders
    params: {rate: 0.02}
  - name: late-arriving-data
    table: orders
    params: {rate: 0.05, delay_min: 6h, delay_max: 2d}
expectations:
  - scenario: duplicate-business-key
    failing_tests: ["test.dbt_shop.assert_raw_orders_unique_per_version*"]
  - scenario: late-arriving-data
    failing_tests: ["test.dbt_shop.assert_daily_orders_event_watermark_matches_full*"]
```

- Lastes med en `yaml.SafeLoader` som i tillegg avviser dupliserte nøkler. Valideres strengt:
  ukjente nøkler på alle nivåer er feil med forslag (`difflib`), typer sjekkes, varigheter parses
  med regex `^\d+(s|m|h|d)$`.
- `version`, `seed` og `dataset.as_of` er påkrevd, slik at en config alltid er fullstendig. På CLI
  er standardene `seed` 42 og `as_of` `2026-01-01T00:00:00Z`.
- Feilmeldinger peker på stien: `scenarios[1].params.delay_max: '2days' is not a duration (use e.g. 2d)`.

### 11.2 `check-dbt`

Leser `target/run_results.json` fra en dbt-kjøring mot dataene og sammenligner med
`expectations`:

- Hver `failing_tests`-glob matches mot `unique_id` med `fnmatch`. En glob uten treff i
  `run_results` er en **konfigfeil** (exit 2), ikke en stille «ikke oppdaget».
- Forventet: minst én node per glob har `status` `fail` eller `error`. Globben kan treffe
  tester eller modeller; `schema-drift` vises ofte som en modell med `error`
  (f.eks. `model.dbt_shop.stg_orders`), ikke som en feilende test.
- I tillegg rapporteres alle *andre* feilende tester som «uventet».
- Output: matrise scenario × forventet test × faktisk status. Exit 1 hvis en forventning
  ikke er oppfylt.
- `check-dbt` med en kjøring uten scenarioer (baseline) forventer at alle tester passerer.
- Formatet til `run_results.json` verifiseres mot ekte dbt i integrasjonstestene; ikke anta
  felter som ikke finnes der.

## 12. Dokumentasjon generert fra kode

`docs/scenarios.md` genereres fra `ScenarioSpec` (navn, summary, detects, typical_dbt_test,
parametere med typer og standardverdier) av `python -m datachaos._docs`. En test feiler hvis
fila ikke er oppdatert. Det samme innholdet vises av `datachaos describe`.

## 13. Teststrategi

### 13.1 Unit (rask, ingen dbt)

- `rng`: kjente forventede sekvenser for fast seed (hardkodet i testen); ingen bruk av
  `random`-modulens globale funksjoner (grep-test).
- Generator: baseline-invariantene (unik nøkkel, FK-er gyldige, enum-verdier gyldige,
  ingen tomme strenger, alle `ingested_at < as_of`, events strengt økende per ordre).
- Hvert scenario: parameter-validering (gyldig, ugyldig, ukjent parameter), invariant,
  ledger-innhold, input ikke mutert, eksklusivitet mot et tidligere scenario.
- `select_rows`: avrunding, `rate` som gir 0, `count` for stor.
- Config: hver feilklasse gir forventet melding med sti.
- CSV/JSONL: NULL, Unicode (`Ålesund`), komma og anførselstegn i strenger, desimaler.
- CLI: exit codes for hver kommando, stdout er gyldig JSON med `--output json`.
- Import-grenser (§ 4).

### 13.2 Integrasjon

- `run` med alle 8 scenarioer i én config → `validate --reproduce` gir exit 0.
- Endre én byte i en datafil → `validate` gir exit 1 og peker på fila.
- Samme config to ganger → lik `content_sha256`; annen seed → ulik.
- dbt-eksemplet (krever `dbt-duckdb`, markert `@pytest.mark.dbt`, kjøres i CI men kan
  hoppes over lokalt): baseline → alle tester grønne; hvert scenario → `check-dbt` exit 0.

### 13.3 Golden

`tests/golden/<case>/chaos.yml` med liten config (20 kunder, 50 ordrer, 3 batcher, fast seed) og
forventede filer i `tests/golden/<case>/expected/`. Oppdateres bare eksplisitt med `DATACHAOS_UPDATE_GOLDEN=1 pytest
tests/golden`, og diffen skal leses i review. Golden tests kjøres i hele Python-matrisen.

## 14. Byggefaser for Claude Code

Arbeidsform for hver fase:

1. Les `CLAUDE.md`, denne planen og eksisterende kode/tester som berøres.
2. Skriv en kort plan i chatten (filer som endres, åpne spørsmål). Ved tvil om tolkning av
   planen: spør i stedet for å gjette.
3. Implementer med tester. Kjør `ruff check && ruff format --check && mypy && pytest`.
4. Oppdater `CHANGELOG.md` (Unreleased) og README der brukerrettet oppførsel endres.
5. Commit med én tydelig melding per fase (eller per scenario i fase 3–4). Stopp og
   rapporter: hva er gjort, hva er testet, hva er avvik fra planen og hvorfor.

Ikke gå videre til neste fase før akseptkriteriene er oppfylt. Ikke svekk eller slett
tester for å få grønt.

### Fase 0 — Repo-oppsett

- `pyproject.toml` (hatchling, `dependencies = ["pyyaml>=6"]`, dev: pytest, ruff, mypy,
  jsonschema, types-PyYAML), `[project.scripts] datachaos = "datachaos.cli:main"`.
- ruff/mypy/pytest-konfig kopiert fra dataowl. `src/datachaos/{__init__,__main__,cli,errors}.py`.
- `.github/workflows/test.yml` (matrise 3.10–3.13), `LICENSE` (MIT), `CHANGELOG.md`,
  README-skjelett, `CLAUDE.md` (følger med denne planen), `.gitignore`.
- **Akseptanse:** `pip install -e ".[dev]"`, `datachaos --version`, og alle fire
  kvalitetskommandoer går grønt i CI.

### Fase 1 — Modell, RNG, generator, skriving

- `rng.py`, `timeutil.py`, `model/*`, `datasets/shop.py`, `io/*`, `engine.py` (uten
  scenarioer), `generate`-kommandoen, manifest 1.0 + JSON Schema.
- **Akseptanse:** `datachaos generate --out a --seed 1 && datachaos generate --out b --seed 1`
  gir identiske filer (`diff -r`, ekskl. `created_at`); baseline-invariantene testet;
  golden case `baseline_small` på plass; manifest validerer mot schema.

### Fase 2 — Scenario-rammeverk og tre enkle scenarioer

- `scenarios/base.py`, registry, `select_rows`, ledger, eksklusivitet, config-lasting,
  `run` (config og CLI-variant), `list-scenarios`, `describe`, `docs/scenarios.md`-generator.
- Scenarioer: `duplicate-business-key`, `null-critical-column`, `invalid-enum`.
- **Akseptanse:** config med alle tre gir reproduserbart resultat; hver har
  invariant-, parameter- og golden-test; `docs/scenarios.md`-testen går.

### Fase 3 — Referanse og tid

- `orphaned-foreign-key`, `late-arriving-data`, `out-of-order-events`, `historical-update`.
- Batch-flytting via `ingested_at`, validering mot `as_of`.
- **Akseptanse:** som fase 2, pluss test av at sene rader havner i riktig senere batch og at
  for stor `delay_max` gir valideringsfeil før noe skrives.

### Fase 4 — Schema-drift

- `schema-drift` med de fire operasjonene, schema per batch, konflikt-sjekk mot andre
  scenarioer.
- **Akseptanse:** batcher før `from_batch` byte-identiske med kjøring uten drift (test),
  `--combined` nekter med forklarende melding når schema varierer.

### Fase 5 — validate, report, inject, land

- `validate` (inkl. `--reproduce`), `report`, `inject`, `land`, `--output json` overalt,
  exit codes.
- **Akseptanse:** integrasjonstestene i § 13.2 unntatt dbt-delen går grønt.

### Fase 6 — dbt-eksempel og `check-dbt`

`examples/dbt_shop/` (dbt-duckdb, `profiles.yml` i prosjektmappa med relativ sti, som i
DAGtracers `testproject/`):

- Sources leser landing-mappa med DuckDB `read_csv('<landing>/<table>/*.csv',
  union_by_name=true, filename=true)` via en liten makro eller `external`-kilde. Verifiser
  hvilken mekanisme dbt-duckdb-versjonen faktisk støtter før du velger.
- Modeller: `stg_customers`, `stg_orders` (dedup på siste `updated_at`), `stg_order_events`
  (dedup på `event_id`), `fct_orders` (inner join mot kunder), og sårbar/robust-par for de
  to tidsscenarioene:
  - `daily_orders_event_watermark` (inkrementell, vannmerke på `order_timestamp`, sårbar)
    og `daily_orders_ingest_watermark` (vannmerke på `ingested_at`, robust);
  - `order_status_by_ingestion` (siste event etter `ingested_at`, sårbar) og
    `order_status_by_event_time` (siste event etter `event_time`, robust).
- Tester:
  - Generiske `not_null`/`relationships`/`accepted_values` på kilder og staging, `unique`
    på staging-nøkler.
  - Rålaget har to rader per nøkkel ved `historical-update` (legitimt), så duplikater fanges
    der med singular test `assert_raw_orders_unique_per_version` (unik på
    `order_id, updated_at`). Uten dbt-pakker; ikke `dbt_utils`.
  - Singular test per inkrementell variant som sammenligner med en full beregning, og per
    status-variant som sammenligner med fasit utledet fra `event_time`. De sårbare skal feile
    under sine scenarioer, de robuste ikke.
- `examples/dbt_shop/run_demo.py`: for hvert scenario (og baseline): `datachaos run`,
  for k = 0..N: `datachaos land --upto-batch k` + `dbt build`, deretter `check-dbt`.
  Skriver en samlet tabell.
- `dbtcheck.py` og `check-dbt`-kommandoen.
- **Akseptanse:** `python examples/dbt_shop/run_demo.py` kjører fra rent miljø med
  `pip install -e ".[dev]"`, baseline er grønn, og hvert scenario fanges av forventet test.
  De sårbare modellene fanges av `late-arriving-data` og `out-of-order-events`; de robuste
  gjør det ikke.

### Fase 7 — Release-klar

- README: hva, hvorfor, 5-minutters quickstart, scenario-tabell, output-format, NULL-regel,
  determinisme-garanti og hva den ikke dekker, eksempel på hele løkken.
- `.github/workflows/publish.yml` (kopier fra dataowl; trusted publishing).
- Test i CI som bygger wheel og sdist og installerer wheel i rent venv, kjører
  `datachaos generate` og `validate`.
- Sjekk at navnet `datachaos` er ledig på PyPI før første release. Hvis ikke: behold
  importnavn og CLI, velg et annet distribusjonsnavn.
- **Akseptanse:** «Definition of Done» under.

### Definition of Done — MVP (0.1.0)

- `pip install datachaos` i rent miljø; `datachaos --help` og alle underkommandoer fungerer.
- `shop`-datasettet med tre tabeller og batcher genereres deterministisk på 3.10–3.13.
- Alle åtte scenarioer har spesifikasjon, invariant, ledger, golden test og dokumentasjon.
- `validate --reproduce` beviser reproduserbarhet fra manifestet alene.
- dbt-demoen viser hele løkken, og `check-dbt` gir exit 0 for alle scenarioer.
- Ingen input skrives over; output skrives atomisk.

## 15. Etter MVP (ikke start før DoD er nådd)

Prioritert:

1. Flere scenarioer: `replayed-events`, `future-timestamp`, `missing-batch`
   (hel batch mangler), `partial-batch`, `timezone-shift`, `truncated-string`,
   `cdc-delete-missing`, `cdc-update-before-insert`. Hvert med samme spesifikasjonsmal.
2. Brukerdefinerte tabeller via schema-fil (YAML med kolonner, typer, roller og
   enkle generatorer) i stedet for bare `shop`. Dette er det som åpner for brukerens egne data.
3. Parquet-output som extra (`pip install datachaos[parquet]`, PyArrow).
4. Verktøykjede med søsterprosjektene, uten felles kode:
   - `datachaos report --for-dgt` skriver ferdige `dgt`-kommandoer for nøkler i ledgeren,
     f.eks. `dgt fct_orders -c order_id -v O000052`, så brukeren ser hvor den injiserte
     feilen stopper i DAG-en.
   - DataChaos-output kan bli testfixture for dgt sitt eget testprosjekt.
5. Databricks-skriving som extra, med krav: eksplisitt `--allow-databricks-write`, kun til
   et angitt test-schema, dry-run som standard, aldri overskriving av eksisterende tabell
   uten `--overwrite`, logg av alle writes. Først da kan dataowl kjøres mot kaos-tabeller.

## 16. Hva som er endret fra ChatGPT-versjonen, og hvorfor

| Endring | Hvorfor |
|---|---|
| Batcher i datamodellen fra start | Late-arriving, historiske endringer og inkrementelle dbt-modeller kan ikke testes med én fil. Å ettermontere batcher senere ville kreve omskriving av alle scenarioer. |
| Ny tabell `order_events` | «Out-of-order» i en snapshot-tabell er meningsløst (SQL har ingen radrekkefølge). Det gir bare mening i en eventlogg på tvers av batcher. |
| Eget `Rng` bygd på `random()` | `random.Random.choice/sample/shuffle` er ikke garantert like på tvers av Python-versjoner; ChatGPT-planen lovet determinisme den ikke kunne holde. |
| `as_of` i stedet for «nå» | `historical-update` («eldre enn N dager») og fremtidige tidsstempler må være relative til et fast tidspunkt for å være deterministiske. |
| Ledger per endring | Antall berørte rader er ikke nok til å validere eller til å feilsøke. Ledgeren gir konkrete nøkler, som også kan spores med `dgt`. |
| `check-dbt` i MVP | Kjerneverdien er «oppdaget pipelinen feilen?». Uten dette må brukeren sjekke manuelt. |
| Eksklusivitet mellom scenarioer | Uten regel kan scenario 3 overskrive scenario 1, og rapporten blir feil uten at noen merker det. |
| `historical-update` legger til ny versjon | En ekte kilde sender en ny versjon; den endrer ikke gamle rader på stedet. |
| `invalid-enum` inn i MVP, `schema-drift` per batch | `accepted_values` er en av de fire innebygde dbt-testene. Schema-drift skjer mellom leveranser, ikke midt i en fil. |
| `pyyaml` som eneste dependency | ChatGPT-planen krevde både «ingen runtime-avhengigheter» og `yaml.safe_load`. |
| Exit codes 0/1/2 | Samme som `dgt`, slik at verktøyene oppfører seg likt i CI. |
| Én kjøring per fase, med stopp og rapport | Gjør hver fase reviewbar og hindrer at Claude Code bygger videre på en feil tolkning. |