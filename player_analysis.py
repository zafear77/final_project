from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round
from pyspark.sql import functions as F
import pandas as pd
import glob

spark = SparkSession.builder \
    .appName("Matriks Pemain Berdasarkan Posisi") \
    .config("spark.jars", "/home/zaqiy/postgresql-42.7.4.jar") \
    .getOrCreate()

# Mengambil data dari postgresql
postgres_url = "jdbc:postgresql://172.25.64.87:5432/pentaho_db"
df = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", "player_data") \
    .option("user", "zaqiy") \
    .option("password", "ZANDROMAX") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def calculate_fitness_score(df):
    # Menghitung faktor games_injured
    df = df.withColumn("fitness_score", 
                       F.when(F.col("games_injured") <= 5, 8)
                       .when(F.col("games_injured") <= 10, 6)
                       .otherwise(4))

    # Menghitung faktor days_injured
    df = df.withColumn("days_injured_factor", F.when(F.col("days_injured") < 100, 1) 
                       .when(F.col("days_injured") < 200, 0.9)
                       .when(F.col("days_injured") < 300, 0.7)
                       .otherwise(0.5)) 

    df = df.withColumn("fitness_score", 
                       F.col("fitness_score") * F.col("days_injured_factor"))

    # Menghitung faktor usia
    df = df.withColumn("fitness_score", 
                       F.when(F.col("age") > 30, F.col("fitness_score") - 0.2 * (F.col("age") - 30))
                       .otherwise(F.col("fitness_score")))

    df = df.withColumn("fitness_score", 
                       F.when(F.col("fitness_score") < 0, 0).otherwise(F.col("fitness_score")))

    return df

df = calculate_fitness_score(df)

# Pembobotan Matriks posisi pemain 
position_metrics = {
    "GK": {"metrics": ["goals_conceded_percentage", "clean_sheets_percentage", "games_injured", "minutes_played", "fitness_score"], "weights": [0.3, 0.3, 0.2, 0.1, 0.1]},
    "CB": {"metrics": ["goals_conceded_percentage", "clean_sheets_percentage", "yellow_cards_percentage", "minutes_played", "fitness_score"], "weights": [0.25, 0.35, 0.2, 0.1, 0.1]},
    "RB": {"metrics": ["assists_percentage", "clean_sheets_percentage", "minutes_played", "awards", "fitness_score"], "weights": [0.25, 0.25, 0.2, 0.2, 0.1]},
    "LB": {"metrics": ["assists_percentage", "clean_sheets_percentage", "minutes_played", "awards", "fitness_score"], "weights": [0.25, 0.25, 0.2, 0.2, 0.1]},
    "DMF": {"metrics": ["assists_percentage", "yellow_cards", "minutes_played", "awards", "fitness_score"], "weights": [0.3, 0.2, 0.2, 0.2, 0.1]},
    "CMF": {"metrics": ["assists_percentage", "goals_percentage", "minutes_played", "awards", "fitness_score"], "weights": [0.3, 0.3, 0.2, 0.1, 0.1]},
    "AMF": {"metrics": ["assists_percentage", "goals_percentage", "minutes_played", "awards", "fitness_score"], "weights": [0.3, 0.3, 0.2, 0.1, 0.1]},
    "RMF": {"metrics": ["assists_percentage", "minutes_played", "goals_percentage", "awards", "fitness_score"], "weights": [0.3, 0.2, 0.3, 0.1, 0.1]},
    "LMF": {"metrics": ["assists_percentage", "minutes_played", "goals_percentage", "awards", "fitness_score"], "weights": [0.3, 0.2, 0.3, 0.1, 0.1]},
    "CF": {"metrics": ["goals_percentage", "assists_percentage", "minutes_played", "awards", "fitness_score"], "weights": [0.4, 0.3, 0.2, 0.1, 0.1]},
    "LWF": {"metrics": ["goals_percentage", "assists_percentage", "minutes_played", "winger", "fitness_score"], "weights": [0.4, 0.3, 0.2, 0.05, 0.05]},
    "RWF": {"metrics": ["goals_percentage", "assists_percentage", "minutes_played", "winger", "fitness_score"], "weights": [0.4, 0.3, 0.2, 0.05, 0.05]},
}

# Hitung Overall Score
def add_position_metrics(df):
    overall_score = lit(0)  
    
    for position, details in position_metrics.items():
        metrics = details["metrics"]
        weights = details["weights"]
        
        # Filter kolom yang tersedia di DataFrame
        available_metrics = [metric for metric in metrics if metric in df.columns]
        available_weights = [weights[metrics.index(metric)] for metric in available_metrics]
        
        if available_metrics:
            # Menghitung weighted sum hanya jika posisi pemain sesuai
            weighted_sum = lit(0)
            for metric, weight in zip(available_metrics, available_weights):
                weighted_sum += col(metric) * lit(weight)
            
            # Tentukan apakah pemain sesuai dengan posisi dan sesuaikan skor overall_score
            overall_score = when(col("position") == position, weighted_sum).otherwise(overall_score)

    # Menambahkan kolom overall_score dan menghapus kolom days_injured_factor
    df = df.withColumn("overall_score", overall_score)
    df = df.drop("days_injured_factor")
    return df

# Mengaplikasikan fungsi untuk menghitung skor keseluruhan per posisi
df = add_position_metrics(df)
df = df.withColumn("overall_score", round(col("overall_score"), 1))
df = df.withColumn("currency", lit("Euro"))

# Menyimpan hasil ke format CSV 
output_path = "/home/zaqiy/hasil_overall_skor"
df.coalesce(1).write.option("header", "true").csv(output_path, mode="overwrite")

# Mendapatkan file hasil dari direktori output
csv_file = glob.glob(f"{output_path}/part-*.csv")[0] 
pandas_df = pd.read_csv(csv_file)
print(pandas_df.head())

# Hasil output file csv
final_csv_path = "/home/zaqiy/overall_score_analysis.csv"
pandas_df.to_csv(final_csv_path, index=False)
print(f"File CSV final berhasil disimpan di: {final_csv_path}")