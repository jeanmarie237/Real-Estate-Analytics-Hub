# real_estate_analytics — Projet dbt

Transformations ELT des données DVF (Demandes de Valeurs Foncières) dans Snowflake.

## Prérequis

- dbt-snowflake 1.11.2
- Profil `real_estate_analytics` configuré dans `~/.dbt/profiles.yml`
- Accès Snowflake : base `DVF_DB`, rôle `DVF_BI_ROLE`, warehouse `DVF_WH`

## Commandes essentielles

> Toujours lancer depuis ce répertoire (`real_estate_analytics/`).

```bash
dbt debug          # Vérifier la connexion Snowflake
dbt deps           # Installer les packages (dbt_utils)
dbt run            # Builder tous les modèles
dbt test           # Lancer les tests de qualité
dbt docs generate  # Générer la documentation
dbt docs serve     # Servir la doc sur http://localhost:8080
```

## Architecture Medallion

```
BRONZE (vue)          SILVER (incremental)       GOLD (tables)
─────────────         ────────────────────       ──────────────────────
src_dvf          ──►  silver_mutation_f     ──►  top_communes
                                                 prix_moyen_commune
                                                 prix_m2_commune
                                                 repartition_types
                                                 volume_mensuel
                                                 surface_vs_prix
```

## Modèles

### Staging — BRONZE

**`src_dvf`** (vue sur `DVF_DB.DEV_BRONZE.mutations_foncieres`)

Renomme les colonnes source en snake_case. Point d'entrée unique pour tous les modèles aval.

Colonnes clés : `no_disposition`, `date_mutation`, `nature_mutation`, `valeur_fonciere`, `commune`, `code_postal`, `type_local`, `surface_reelle_bati`, `surface_terrain`

---

### Silver

**`silver_mutation_f`** (incremental, `unique_key='mutation_id'`)

Nettoyage complet des données brutes :
- `TRY_TO_DATE` / `TRY_TO_NUMBER` pour le casting
- `REPLACE(',', '.')` pour les décimales françaises
- `LOWER()` pour la normalisation des strings
- `ROW_NUMBER()` par mutation + lot pour gérer les multi-biens
- `MD5(...)` sur 11 champs → `mutation_id` unique
- `prix_metre_carre` = `valeur_fonciere / surface_terrain`

Stratégie incrémentale : seules les nouvelles `mutation_id` sont insérées.

---

### Gold

| Modèle | Grain | KPIs |
|---|---|---|
| `top_communes` | commune | prix_moyen (min. 50 transactions) |
| `prix_moyen_commune` | commune × type_local | nb_transactions, prix_moyen, prix_median |
| `prix_m2_commune` | commune × type_local | nb_transactions, prix_m2_moyen |
| `repartition_types` | type_local | nb_transactions |
| `volume_mensuel` | mois | nb_transactions, volume_financier |
| `surface_vs_prix` | type_local × tranche surface | prix_moyen |

## Tests

Résultats attendus : **PASS=10 / WARN=3 / ERROR=0**

| Test | Sévérité | Note |
|---|---|---|
| `mutation_id` unique | ERROR | Garanti par MD5 + ROW_NUMBER |
| `mutation_id` not_null | ERROR | |
| `commune` not_null (x3) | ERROR | |
| `prix_moyen` not_null | ERROR | Filtré en amont (`WHERE valeur_fonciere IS NOT NULL`) |
| `prix_m2_moyen` not_null | ERROR | Filtré en amont |
| `mois` / `nb_transactions` not_null | ERROR | |
| `valeur_fonciere` not_null | WARN | Normal : donations/échanges sans prix |
| `type_local` not_null | WARN | Normal : terrains sans construction |
| `prix_metre_carre` not_null | WARN | Normal : transactions sans surface bâtie |

## Packages

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.3
```

## Schéma Snowflake cible

```
DVF_DB
├── DEV_BRONZE   → src_dvf (vue)
├── DEV_SILVER   → silver_mutation_f
└── DEV_GOLD     → top_communes, prix_moyen_commune, prix_m2_commune,
                   repartition_types, volume_mensuel, surface_vs_prix
```
