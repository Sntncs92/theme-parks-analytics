# 🎢 Theme Parks Wait Time Analytics

> **Predicting theme park attraction wait times using 5M+ real observations and LightGBM**  
> *Real-time data collection · Exploratory analysis · Machine learning · 40 parks worldwide*

---

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![LightGBM](https://img.shields.io/badge/LightGBM-Model-success?style=flat-square)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)
![Parks](https://img.shields.io/badge/Parks-40%20worldwide-orange?style=flat-square)
![Records](https://img.shields.io/badge/Records-5M%2B-blue?style=flat-square)

</div>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Results](#key-results)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Exploratory Data Analysis](#exploratory-data-analysis)
- [Machine Learning Model](#machine-learning-model)
- [Setup & Usage](#setup--usage)
- [Roadmap](#roadmap)
- [Author](#author)

---

## Overview

This project builds a complete analytics and machine learning pipeline to **predict wait times at theme park attractions worldwide** — enabling visitors to plan smarter, minimize queues, and make the most of their park experience.

The system collects live data from **40 theme parks** every 15 minutes, stores it in a PostgreSQL database, and uses a LightGBM model trained on **5M+ historical observations** to generate wait time predictions.

### Parks Covered
- 🏰 **Disney:** Magic Kingdom, EPCOT, Hollywood Studios, Animal Kingdom, Disneyland CA, Disneyland Paris, Tokyo DisneySea, Shanghai, Hong Kong
- 🎬 **Universal:** Studios Florida, Islands of Adventure, Epic Universe
- 🚩 **Six Flags:** Magic Mountain, Over Texas, México
- 🌍 **Europe:** Efteling, Europa-Park, Alton Towers, PortAventura, Phantasialand, Parc Astérix, Gardaland, Liseberg, Parque Warner Madrid and more
- 🌏 **Others:** Dollywood, Hersheypark, SeaWorld Orlando, Warner Bros. Movie World (Australia)

---

## Key Results

| Metric | Value |
|--------|-------|
| **MAE (Test set)** | **~4.5 minutes** |
| **MAE (Validation)** | **3.10 minutes** |
| **Improvement vs. baseline** | **68%** |
| **Training records** | ~2.7M (status: OPERATING, 1–120 min) |
| **Data range** | Oct 2025 – Apr 2026 |
| **Features used** | 15 (temporal, lag, rolling, park, tier, events) |

> The model comfortably outperforms a naive "average by hour" baseline across all 40 parks.

---

## Project Structure

```
theme_parks/
├── data/
│   └── sample_100k.csv            # 100k representative records (all 40 parks)
├── eda/
│   └── notebooks/
│       ├── 01_overview.ipynb          # Dataset summary and data quality
│       ├── 02_temporal_patterns.ipynb # Hourly, daily, seasonal trends
│       ├── 03_park_comparison.ipynb   # Disney vs Universal vs Six Flags
│       ├── 04_ride_analysis.ipynb     # Most popular and most problematic rides
│       ├── 05_geographic.ipynb        # Continent and country-level insights
│       └── 06_events.ipynb            # Special event impact analysis
├── ml/
│   ├── notebooks/
│   │   └── 01_modeling.ipynb          # Full modeling walkthrough
│   ├── src/
│   │   ├── features.py                # FEATURES list, build_features(), drop_nulls()
│   │   ├── train.py                   # Model training and serialization
│   │   └── predict.py                 # Load model and generate predictions
│   ├── models/
│   │   ├── lgbm_v1.pkl                # Trained LightGBM model
│   │   ├── baseline_dict.pkl          # Baseline (mean by hour/park)
│   │   └── park_mapping.pkl           # Park name → encoded integer
│   └── requirements.txt
└── outputs/                           # Generated charts and exports
```

---

## Data Pipeline

> **Note:** The data collection infrastructure lives in a [separate repository](https://github.com/Sntncs92) — this repo focuses on analysis and modeling.

The collection system runs 24/7 on a Hetzner Cloud server (Ubuntu 24.04) and polls a theme park API every 15 minutes. Data is written directly to PostgreSQL 16.

### Database Schema

```sql
parks       → park_id, park_name, country, continent
rides       → ride_id (UUID), park_id, ride_name, tier, is_active
wait_times  → measurement_id, ride_id, timestamp, status, wait_time, evento
holidays    → date, country, holiday_name
```

**Status values:** `OPERATING` · `CLOSED` · `DOWN` · `REFURBISHMENT`

---

## Exploratory Data Analysis

Six notebooks covering:

- **Temporal patterns** — Morning rush, lunch dip, evening peak. Weekday vs. weekend differences (weekends run ~35% longer on average).
- **Park comparisons** — Disney parks average the highest wait times; Six Flags shows the most volatility.
- **Ride-level analysis** — Top 20 longest queues, most unreliable attractions, downtime percentage by park.
- **Geographic insights** — North American parks vs. European parks vs. Asia-Pacific.
- **Event impact** — How special events (Halloween, Christmas, etc.) shift wait times across the day.

---

## Machine Learning Model

### Features (15 total)

| Category | Features |
|----------|----------|
| Temporal | `hour`, `day_of_week`, `month`, `is_weekend` |
| Calendar | `is_holiday`, `has_event` |
| Park | `park_encoded`, `tier` |
| Lag | `wait_t1`, `wait_t2`, `wait_t3` (previous measurements) |
| Rolling | `roll_mean_1h`, `roll_mean_3h`, `roll_max_1h`, `roll_std_3h` |

### Training

```python
# Temporal split — no data leakage
Train:      Oct 2025 – Jan 2026  (~80%)
Validation: Feb 2026             (~10%)
Test:       Mar 2026             (~10%)
```

### Model performance

```
Baseline MAE (mean by hour):   ~14.1 min
LightGBM MAE (validation):      3.10 min
LightGBM MAE (test):            ~4.5 min
Improvement:                      68%
```

---

## Setup & Usage

### Prerequisites

- Python 3.12+

### Installation

```bash
git clone https://github.com/Sntncs92/theme-parks-analytics.git
cd theme-parks-analytics

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS / Linux

pip install -r ml/requirements.txt
```

### Running the notebooks

A representative sample of **100,000 real observations** is included in `data/sample_100k.csv` — covering all 40 parks with the most recent records available. No database access needed.

```
data/
└── sample_100k.csv   # 100k rows · 40 parks · Oct 2025 – Apr 2026
```

Simply open any notebook in `eda/` or `ml/notebooks/` and point the data loading cell to this file.

> **Note:** The full dataset (5M+ records) lives in a private PostgreSQL database on Hetzner. If you need access for research purposes, feel free to reach out.

---

## Roadmap

- [x] Phase 1 — Data infrastructure & 24/7 collector
- [x] Phase 2 — Exploratory Data Analysis (6 notebooks)
- [x] Phase 3 — LightGBM model (MAE ~4.5 min)
- [ ] Phase 4 — Interactive public dashboard (Streamlit → React)
- [ ] Phase 5 — Public REST API (FastAPI)
- [ ] Phase 6 — Portfolio documentation & case study

---

## Author

**Daniel Frases**  
Data Scientist · ML Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-daniel--frases-0A66C2?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/daniel-frases/)
[![GitHub](https://img.shields.io/badge/GitHub-Sntncs92-181717?style=flat-square&logo=github)](https://github.com/Sntncs92)

---

---

# 🎢 Theme Parks Wait Time Analytics *(Versión en Español)*

> **Predicción de tiempos de espera en atracciones de parques temáticos con 5M+ observaciones reales y LightGBM**  
> *Recogida de datos en tiempo real · Análisis exploratorio · Machine learning · 40 parques en todo el mundo*

---

## Descripción general

Este proyecto construye un pipeline completo de analítica y machine learning para **predecir los tiempos de espera en atracciones de parques temáticos de todo el mundo**, ayudando a los visitantes a planificar mejor sus visitas y minimizar las colas.

El sistema recoge datos en vivo de **40 parques temáticos** cada 15 minutos, los almacena en PostgreSQL y utiliza un modelo LightGBM entrenado con más de **5 millones de observaciones históricas** para generar predicciones.

### Parques incluidos
- 🏰 **Disney:** Magic Kingdom, EPCOT, Hollywood Studios, Animal Kingdom, Disneyland CA, Disneyland París, Tokyo DisneySea, Shanghái, Hong Kong
- 🎬 **Universal:** Studios Florida, Islands of Adventure, Epic Universe
- 🚩 **Six Flags:** Magic Mountain, Over Texas, México
- 🌍 **Europa:** Efteling, Europa-Park, Alton Towers, PortAventura, Phantasialand, Parc Astérix, Gardaland, Liseberg, Parque Warner Madrid y más
- 🌏 **Otros:** Dollywood, Hersheypark, SeaWorld Orlando, Warner Bros. Movie World (Australia)

---

## Resultados principales

| Métrica | Valor |
|---------|-------|
| **MAE (Test)** | **~4.5 minutos** |
| **MAE (Validación)** | **3.10 minutos** |
| **Mejora vs. baseline** | **68%** |
| **Registros de entrenamiento** | ~2.7M (status: OPERATING, 1–120 min) |
| **Rango temporal** | Oct 2025 – Abr 2026 |
| **Features utilizadas** | 15 (temporales, lag, rolling, parque, tier, eventos) |

---

## Pipeline de datos

> **Nota:** La infraestructura de recogida de datos vive en un [repositorio separado](https://github.com/Sntncs92). Este repo se centra en el análisis y el modelado.

El sistema de recogida corre 24/7 en un servidor Hetzner Cloud (Ubuntu 24.04) y consulta una API de parques temáticos cada 15 minutos, escribiendo los datos directamente en PostgreSQL 16.

---

## Análisis Exploratorio (EDA)

Seis notebooks que cubren:

- **Patrones temporales** — Picos matutinos, bajón al mediodía, pico vespertino. Los fines de semana registran de media un 35% más de espera.
- **Comparativas entre parques** — Los parques Disney tienen los tiempos más altos; Six Flags muestra mayor variabilidad.
- **Análisis por atracción** — Top 20 atracciones con más cola, las menos fiables, porcentaje de tiempo fuera de servicio.
- **Análisis geográfico** — Norteamérica vs. Europa vs. Asia-Pacífico.
- **Impacto de eventos** — Cómo Halloween, Navidad y otros eventos especiales alteran los tiempos a lo largo del día.

---

## Modelo de Machine Learning

### Features (15 en total)

| Categoría | Features |
|-----------|----------|
| Temporales | `hour`, `day_of_week`, `month`, `is_weekend` |
| Calendario | `is_holiday`, `has_event` |
| Parque | `park_encoded`, `tier` |
| Lag | `wait_t1`, `wait_t2`, `wait_t3` |
| Rolling | `roll_mean_1h`, `roll_mean_3h`, `roll_max_1h`, `roll_std_3h` |

### División temporal (sin data leakage)

```
Entrenamiento:  Oct 2025 – Ene 2026  (~80%)
Validación:     Feb 2026             (~10%)
Test:           Mar 2026             (~10%)
```

### Rendimiento del modelo

```
Baseline MAE (media por hora):   ~14.1 min
LightGBM MAE (validación):        3.10 min
LightGBM MAE (test):              ~4.5 min
Mejora:                             68%
```

---

## Instalación y uso

### Requisitos

- Python 3.12+

### Instalación

```bash
git clone https://github.com/Sntncs92/theme-parks-analytics.git
cd theme-parks-analytics

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS / Linux

pip install -r ml/requirements.txt
```

### Ejecutar los notebooks

El repositorio incluye una muestra representativa de **100.000 observaciones reales** en `data/sample_100k.csv` — con todos los parques representados y los registros más recientes disponibles. No se necesita acceso a base de datos.

```
data/
└── sample_100k.csv   # 100k filas · 40 parques · Oct 2025 – Abr 2026
```

Abre cualquier notebook de `eda/` o `ml/notebooks/` y apunta la celda de carga de datos a este archivo.

> **Nota:** El dataset completo (5M+ registros) vive en una base de datos PostgreSQL privada en Hetzner. Si necesitas acceso por motivos de investigación, no dudes en contactar.

---

## Hoja de ruta

- [x] Fase 1 — Infraestructura y colector de datos 24/7
- [x] Fase 2 — Análisis exploratorio (6 notebooks)
- [x] Fase 3 — Modelo LightGBM (MAE ~4.5 min)
- [ ] Fase 4 — Dashboard interactivo público (Streamlit → React)
- [ ] Fase 5 — API REST pública (FastAPI)
- [ ] Fase 6 — Documentación de portfolio y caso de estudio

---

## Autor

**Daniel Frases**  
Data Scientist · ML Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-daniel--frases-0A66C2?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/daniel-frases/)
[![GitHub](https://img.shields.io/badge/GitHub-Sntncs92-181717?style=flat-square&logo=github)](https://github.com/Sntncs92)
