# DataChaos — instruksjoner for Claude Code

Byggeplanen ligger i `docs/build-plan.md`. Les den før hver fase. Denne fila er reglene som
gjelder hele tiden.

## Kommandoer

```bash
pip install -e ".[dev]"
ruff check && ruff format --check && mypy && pytest          # alt må være grønt før commit
pytest -m "not dbt"                                          # rask lokal kjøring
DATACHAOS_UPDATE_GOLDEN=1 pytest tests/golden                # kun når endringen er tilsiktet
python -m datachaos._docs                                    # regenerer docs/scenarios.md
```

## Ufravikelige regler

1. **Determinisme.** Bruk aldri `random`-modulens globale funksjoner, `hash()`, systemtid i
   data, eller iterasjon over `set` uten `sorted()`. All tilfeldighet går via
   `datachaos.rng.Rng`, som bare bygger på `random.Random.random()`.
2. **Scenarioer gjør ikke IO** og muterer aldri input. De returnerer ny `Dataset` og
   ledger-linjer.
3. **Ingen nye runtime-avhengigheter** utover `pyyaml` uten at det er avtalt.
4. **Aldri skriv over input**, aldri skriv utenfor output-mappa, alltid via temp-mappe og
   rename.
5. **Endrer du utvalg eller transformasjon i et scenario**, øk `ScenarioSpec.version`, oppdater
   golden-filene eksplisitt og skriv det i `CHANGELOG.md`.
6. **Ikke svekk eller slett tester** for å få grønt. Er en test feil, si det og forklar hvorfor.

## Arbeidsform

- Én fase av byggeplanen per økt. Start med en kort plan i chatten: filer, rekkefølge, åpne
  spørsmål. Ved tvil om tolkning av planen: spør.
- Avslutt fasen med: kvalitetskommandoene grønne, `CHANGELOG.md` (Unreleased) oppdatert,
  commit, og en kort rapport: hva er gjort, hva er testet, avvik fra planen og hvorfor.
- Ikke start neste fase uten klarsignal.

## Konvensjoner (felles med dataowl og dgt)

- `from __future__ import annotations` i alle moduler, type hints overalt, dataclasses
  (frozen der det går), små moduler.
- Exit codes: `0` OK, `1` validering/forventning feilet, `2` bruks-/konfig-/IO-/verktøyfeil.
- Med `--output json` er stdout kun JSON; status og advarsler til stderr.
- Feilmeldinger: `datachaos: error: <én linje>`; stacktrace bare med `--verbose`.
- Maskinlesbare dokumenter har `schema_version` og `tool_version`.