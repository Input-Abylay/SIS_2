"""Stage 6 - exploratory data analysis on the data STORED IN SQLite.

Reading from output.db (not from the CSV) proves the database holds the expected data.
Text results  -> results/eda_summary.txt
Figures       -> figures/*.png

Research questions
  Q1 How are magnitudes distributed (Gutenberg-Richter: many small, few large)?
  Q2 Where do the events happen and which regions are most active?
  Q3 How does depth relate to magnitude?
  Q4 How did seismic activity change day by day during the month?

Run alone:  python -m src.analysis
"""
import io
import logging
import sqlite3

import matplotlib

matplotlib.use("Agg")    # no window needed - figures are written to files
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

from src import config  # noqa: E402

log = logging.getLogger(__name__)

# Palette: categorical slots for <= 3 series, one sequential hue for magnitudes/counts
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
DEPTH_COLORS = {"shallow": BLUE, "intermediate": ORANGE, "deep": AQUA}
DEPTH_ORDER = ["shallow", "intermediate", "deep"]
MAG_ORDER = ["minor", "light", "moderate", "strong", "major"]
TEXT, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "figure.dpi": 110, "savefig.dpi": 150, "savefig.bbox": "tight",
    "axes.edgecolor": GRID, "axes.labelcolor": TEXT, "axes.titleweight": "bold",
    "axes.titlesize": 13, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.axisbelow": True,
    "xtick.color": MUTED, "ytick.color": MUTED, "legend.frameon": False,
    "font.size": 10,
})


def load_from_db() -> pd.DataFrame:
    connection = sqlite3.connect(config.DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM earthquakes", connection)
    finally:
        connection.close()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["tsunami_flag"] = df["tsunami_flag"].astype(bool)
    return df


def save(fig: plt.Figure, name: str) -> str:
    path = config.FIGURES_DIR / name
    fig.savefig(path)
    plt.close(fig)
    return path.name


def plot_magnitude_distribution(df: pd.DataFrame) -> str:
    """Q1 - histogram on a log scale: straight-ish decline = Gutenberg-Richter law."""
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bins = np.arange(2.5, df["magnitude"].max() + 0.2, 0.1)
    ax.hist(df["magnitude"], bins=bins, color=BLUE, edgecolor="white", linewidth=1)
    ax.set_yscale("log")
    median = df["magnitude"].median()
    ax.axvline(median, color=TEXT, linestyle="--", linewidth=1)
    ax.text(median + 0.05, 0.97, f"median M{median:.1f}", transform=ax.get_xaxis_transform(),
            va="top", color=TEXT, bbox={"facecolor": "white", "edgecolor": "none", "pad": 1})
    ax.set_title("Magnitude distribution of M2.5+ events, October 2026")
    ax.set_xlabel("Magnitude")
    ax.set_ylabel("Number of events (log scale)")
    return save(fig, "fig1_magnitude_distribution.png")


def plot_world_map(df: pd.DataFrame) -> str:
    """Q2 - epicentres; point size = magnitude, colour = depth (single sequential hue)."""
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ordered = df.sort_values("magnitude")
    points = ax.scatter(ordered["longitude"], ordered["latitude"],
                        s=(ordered["magnitude"] - 2) ** 2.6 * 4,
                        c=ordered["depth_km"], cmap="Blues", vmin=-100, vmax=650,
                        edgecolor="white", linewidth=0.4, alpha=0.9)
    colorbar = fig.colorbar(points, ax=ax, pad=0.01, shrink=0.85)
    colorbar.set_label("Depth (km)")
    colorbar.outline.set_visible(False)
    for magnitude in (3, 5, 7):
        ax.scatter([], [], s=(magnitude - 2) ** 2.6 * 4, color=BLUE, label=f"M{magnitude}")
    ax.legend(title="Magnitude", loc="lower left", labelspacing=1.2)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-90, 90)
    ax.set_title("Earthquake epicentres (size = magnitude, colour = depth)")
    ax.set_xlabel("Longitude (degrees)")
    ax.set_ylabel("Latitude (degrees)")
    return save(fig, "fig2_epicentre_map.png")


def plot_top_regions(df: pd.DataFrame) -> str:
    """Q2 - the 10 most active regions with their mean magnitude."""
    regions = (df.groupby("region")
                 .agg(events=("event_id", "count"), mean_mag=("magnitude", "mean"))
                 .nlargest(10, "events").iloc[::-1])
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(regions.index, regions["events"], color=BLUE, height=0.65)
    for y, (count, mean_mag) in enumerate(zip(regions["events"], regions["mean_mag"])):
        ax.text(count + 5, y, f"{count}  (mean M{mean_mag:.1f})", va="center", color=MUTED, fontsize=9)
    ax.set_xlim(0, regions["events"].max() * 1.3)
    ax.grid(axis="y", visible=False)
    ax.set_title("Ten most seismically active regions")
    ax.set_xlabel("Number of events")
    ax.set_ylabel("Region")
    return save(fig, "fig3_top_regions.png")


def plot_depth_vs_magnitude(df: pd.DataFrame) -> str:
    """Q3 - scatter of depth against magnitude, coloured by depth class."""
    fig, ax = plt.subplots(figsize=(8, 5))
    for depth_class in DEPTH_ORDER:
        part = df[df["depth_class"] == depth_class]
        ax.scatter(part["magnitude"], part["depth_km"], s=14, color=DEPTH_COLORS[depth_class],
                   edgecolor="white", linewidth=0.4, label=f"{depth_class} (n={len(part)})")
    ax.invert_yaxis()   # depth grows downward, like in the Earth
    ax.set_title("Hypocentre depth vs magnitude")
    ax.set_xlabel("Magnitude")
    ax.set_ylabel("Depth (km, downward)")
    ax.legend(title="Depth class", loc="lower right")
    return save(fig, "fig4_depth_vs_magnitude.png")


def plot_daily_activity(df: pd.DataFrame) -> str:
    """Q4 - daily counts per depth class (built with pivot_table)."""
    daily = pd.pivot_table(df, index="event_date", columns="depth_class",
                           values="event_id", aggfunc="count", fill_value=0)[DEPTH_ORDER]
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for depth_class in DEPTH_ORDER:
        ax.plot(daily.index, daily[depth_class], color=DEPTH_COLORS[depth_class],
                linewidth=2, marker="o", markersize=3, label=depth_class)
    peak_day = daily.sum(axis=1).idxmax()
    ax.annotate(f"busiest day {peak_day:%d %b}:\n{daily.sum(axis=1).max()} events in total",
                xy=(peak_day, daily.loc[peak_day, "shallow"]), xytext=(15, -5),
                textcoords="offset points", color=TEXT, fontsize=9,
                arrowprops={"arrowstyle": "-", "color": MUTED})
    ax.set_ylim(0, daily.max().max() * 1.25)
    ax.set_title("Daily number of events by depth class")
    ax.set_xlabel("Date (UTC)")
    ax.set_ylabel("Events per day")
    ax.legend(title="Depth class", ncol=3, loc="upper left")
    fig.autofmt_xdate()
    return save(fig, "fig5_daily_activity.png")


def plot_class_heatmap(df: pd.DataFrame) -> str:
    """Magnitude class x depth class contingency table (pivot_table) as a heatmap."""
    table = pd.pivot_table(df, index="magnitude_class", columns="depth_class",
                           values="event_id", aggfunc="count", fill_value=0)
    table = table.reindex(index=MAG_ORDER, columns=DEPTH_ORDER, fill_value=0)
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    sns.heatmap(table, annot=True, fmt="d", cmap="Blues", linewidths=2, linecolor="white",
                cbar_kws={"label": "Number of events"}, ax=ax)
    ax.set_title("Events by magnitude class and depth class")
    ax.set_xlabel("Depth class")
    ax.set_ylabel("Magnitude class")
    return save(fig, "fig6_class_heatmap.png")


def build_text_summary(df: pd.DataFrame) -> tuple[str, dict]:
    out = io.StringIO()

    def section(title: str, body) -> None:
        out.write(f"\n=== {title} ===\n{body}\n")

    section("Dimensions", f"{df.shape[0]} rows x {df.shape[1]} columns")
    section("Data types", df.dtypes.to_string())
    numeric_cols = ["magnitude", "depth_km", "significance", "felt_reports", "station_count",
                    "azimuthal_gap", "rms", "distance_to_almaty_km"]
    section("Descriptive statistics (numerical)", df[numeric_cols].describe().round(2).to_string())
    section("Skewness", df[numeric_cols].skew().round(2).to_string())
    section("Correlation magnitude / depth / significance",
            df[["magnitude", "depth_km", "significance", "felt_reports"]].corr().round(2).to_string())
    for column in ["event_type", "status", "network", "mag_type", "depth_class",
                   "magnitude_class", "alert_level"]:
        counts = df[column].value_counts()
        share = (counts / len(df) * 100).round(1)
        section(f"Categorical: {column} (n unique = {df[column].nunique()})",
                pd.DataFrame({"count": counts, "percent": share}).to_string())
    section("Grouped: depth class", df.groupby("depth_class")
            .agg(events=("event_id", "count"), mean_mag=("magnitude", "mean"),
                 max_mag=("magnitude", "max"), mean_sig=("significance", "mean")).round(2).to_string())
    section("Grouped: magnitude type", df.groupby("mag_type")
            .agg(events=("event_id", "count"), mean_mag=("magnitude", "mean"),
                 mean_depth=("depth_km", "mean")).round(2).sort_values("events", ascending=False).to_string())
    section("Grouped: day of week", df.groupby("day_of_week")["event_id"].count()
            .reindex(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
            .to_string())
    nearest = df.nsmallest(5, "distance_to_almaty_km")[
        ["event_time", "magnitude", "place", "distance_to_almaty_km"]]
    section("Five events closest to Almaty", nearest.to_string(index=False))

    # Gutenberg-Richter b-value (maximum-likelihood, Aki 1965) for M >= 4.5 (complete global catalogue)
    completeness = 4.5
    above = df.loc[df["magnitude"] >= completeness, "magnitude"]
    b_value = np.log10(np.e) / (above.mean() - (completeness - 0.05))

    facts = {
        "rows": int(len(df)),
        "mean_magnitude": round(float(df["magnitude"].mean()), 2),
        "median_magnitude": round(float(df["magnitude"].median()), 2),
        "max_magnitude": float(df["magnitude"].max()),
        "strongest_place": df.loc[df["magnitude"].idxmax(), "place"],
        "share_shallow_pct": round(float((df["depth_class"] == "shallow").mean() * 100), 1),
        "top_region": df["region"].value_counts().index[0],
        "top_region_events": int(df["region"].value_counts().iloc[0]),
        "tsunami_flagged": int(df["tsunami_flag"].sum()),
        "events_m5_plus": int((df["magnitude"] >= 5).sum()),
        "b_value_m45": round(float(b_value), 2),
        "corr_mag_depth": round(float(df["magnitude"].corr(df["depth_km"])), 2),
        "nearest_to_almaty_km": float(df["distance_to_almaty_km"].min()),
        "nearest_to_almaty_place": df.loc[df["distance_to_almaty_km"].idxmin(), "place"],
    }
    section("Key facts", "\n".join(f"{k}: {v}" for k, v in facts.items()))
    return out.getvalue(), facts


def run_analysis() -> dict:
    df = load_from_db()
    summary_text, facts = build_text_summary(df)
    (config.RESULTS_DIR / "eda_summary.txt").write_text(summary_text)
    figures = [plot_magnitude_distribution(df), plot_world_map(df), plot_top_regions(df),
               plot_depth_vs_magnitude(df), plot_daily_activity(df), plot_class_heatmap(df)]
    log.info("EDA done: %d figures saved to figures/", len(figures))
    return {"facts": facts, "figures": figures}


if __name__ == "__main__":
    config.setup_logging()
    result = run_analysis()
    print((config.RESULTS_DIR / "eda_summary.txt").read_text())
    print("Figures:", ", ".join(result["figures"]))
