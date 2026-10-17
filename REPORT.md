# Informe d'anàlisi de rendiment: filtre automàtic de ruta a l'arrel

## 1. Versions i entorn

- Llibreria: `guillotina_elasticsearch` commit `ad4eb66` (8.0.15.dev0).
- Guillotina: 7.0.5.
- Python: 3.9.13.
- Client Elasticsearch: 8.12.0.
- Servidor Elasticsearch: 8.12.0 (imatge oficial, 1 shard, 0 rèpliques, heap 1g).
- PostgreSQL: DUMMY storage per a les proves de llibreria.

## 2. Comportament actual

`BaseParser` (Guillotina) calcula `path = get_content_path(context)`. Si el context és l'arrel del contenidor, `path == "/"`. Aquest valor es tradueix a `params["path__starts"] = "/"`.

El parser de la llibreria (`process_field`, modificador `starts`) genera:

- si `value == "/"`: `{"wildcard": {"path": "/*"}}`.

El camp `path` està mapejat com `text` amb l'analitzador `path_analyzer`, definit per `path_hierarchy` amb delimitador `/`. Això produeix tokens jeràrquics (`/a`, `/a/b`, ...), però **no** un token `/` per a rutes niades. Per això el wildcard `/*` coincideix amb tots els termes indexats que comencen per `/`, és a dir, pràcticament tots els documents amb ruta absoluta.

## 3. Cost mesurat

Bateria directa a Elasticsearch amb dades sintètiques deterministes (seed 12345), 1 shard, `track_total_hits=true`:

| Documents | Wildcard `/*` (ms) | Prefix `/` (ms) | Sense filtre (ms) | Exists (ms) |
|---|---:|---:|---:|---:|
| 100.000 | 1,58 (mediana client) | 1,12 | 0,94 | 0,96 |
| 1.000.000 | took 222 | took 178 | ~2 | ~1,3 |

El profile mostra que el component `path:/*` costa ~162 ms a 1M de documents (build_scorer), mentre que eliminar el filtre redueix la consulta a ~2 ms. La causa és l'enumeració de tots els termes de l'índex que comencen per `/`.

## 4. Equivalència de variants

Amb un conjunt de casos límit (ruta buida, relativa, sense camp, arrel, niada, amb barra final), la comparació de resultats (IDs i totals) dóna:

- `wildcard /*` i `prefix /`: **idèntics**.
- `exists(path)`: inclou rutes buides i relatives (no equivalent).
- Sense filtre: inclou documents sense ruta i sense camp (no equivalent).

Per tant, `prefix` és una substitució exacta de baix risc; l'omissió del filtre canvia el comportament per a documents malformats.

## 5. Candidat preparat

S'ha modificat `guillotina_elasticsearch/parser.py` per ometre `path__starts` quan val `"/"` (context arrel). Això elimina el filtre redundant per a contingut vàlid de Guillotina (rutes absolutes) i redueix la latència de la consulta de ~222 ms a ~2 ms a 1M de documents.

Canvi observable: documents amb ruta buida, relativa o sense camp `path` i profunditat >= 1 poden aparèixer en cerques a l'arrel, mentre que abans s'excloïen. Segons el contracte de Guillotina, les rutes de contingut són sempre absolutes i comencen per `/`, de manera que aquest cas no hauria d'ocórrer en operació normal.

## 6. Proves

Les proves existents de `test_search.py` i `test_parser.py` s'executen amb els mateixos resultats que a la branca base (2 fallades preexistents no relacionades amb el canvi). S'han afegit generador de dades i scripts de benchmark a `benchmarks/`.

## 7. Recomanació

- Benefici reproduïble i gran (≈100x a escala) per a cerques a l'arrel.
- Risc de compatibilitat baix per a dades vàlides, però canvi observable per a documents malformats.
- Recomanació: publicar com a **minor/major** segons la política del projecte, documentant el canvi observable. Si es vol conservar l'equivalència exacta, alternativa de baix risc: substituir `wildcard /*` per `prefix /` (≈18-20% més ràpid), apta com a patch.
