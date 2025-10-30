import matplotlib.pyplot as plt
import pandas as pd

class Visualization:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def plot_championships(self):
        # Sort by highest_accolades
        df_sorted = self.df.sort_values('highest_accolades', ascending=False)

        plt.figure(figsize=(10, 6))
        plt.bar(df_sorted['name'], df_sorted['highest_accolades'], color='red')
        plt.xticks(rotation=45, ha='right')
        plt.xlabel('Team')
        plt.ylabel('World Championships')
        plt.title('F1 Teams - World Championships')
        plt.tight_layout()
        plt.show()
