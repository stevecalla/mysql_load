import pandas as pd
import matplotlib.pyplot as plt
import os
import json
import sys
import numpy as np
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages
from wordcloud import WordCloud, STOPWORDS
from sklearn.feature_extraction.text import CountVectorizer
import statsmodels.tsa.stattools as ts

# ----------------------------
# Utility Functions
# ----------------------------

def load_data(file_path):
    """Load CSV data from file_path."""
    return pd.read_csv(file_path, low_memory=True)

def preprocess_data(df):
    """Preprocess the DataFrame:
       - Convert nps_score to numeric.
       - Create an nps_category column.
       - Identify multiple-time renters.
    """
    df['nps_score'] = pd.to_numeric(df['nps_score'], errors='coerce')
    
    def categorize_nps(score):
        if score >= 9:
            return 'Promoter'
        elif score >= 7:
            return 'Passive'
        elif score >= 0:
            return 'Detractor'
        return 'Unknown'
    
    df['nps_category'] = df['nps_score'].apply(categorize_nps)
    df['is_multiple_renter'] = df.duplicated(subset=['user_ptr_id'], keep=False)
    return df

def calculate_nps_breakdown(data):
    """Calculate breakdown statistics for a given DataFrame group."""
    promoters = (data['nps_category'] == 'Promoter').sum()
    passives = (data['nps_category'] == 'Passive').sum()
    detractors = (data['nps_category'] == 'Detractor').sum()
    total_responses = len(data)
    nps_score = None
    if total_responses != 0:
        nps_score = ((promoters - detractors) / total_responses) * 100
    return pd.Series({
        'nps_score': nps_score,
        'total_responses': total_responses,
        'promoters': promoters,
        'passives': passives,
        'detractors': detractors
    })

def clean_return_data(df, current_month):
    """Clean and filter return month data and compute breakdown."""
    df['return_year_month_dt'] = pd.to_datetime(df['return_year_month'], format='%Y-%m', errors='coerce')
    df_clean = df[(df['return_year_month_dt'].notnull()) & (df['return_year_month_dt'] <= current_month)].copy()
    df_clean['return_date_month'] = df_clean['return_year_month_dt'].dt.to_period('M').astype(str)
    breakdown = df_clean.groupby(['return_date_month', 'is_multiple_renter'])\
                        .apply(calculate_nps_breakdown).reset_index()
    breakdown = breakdown[breakdown['total_responses'] >= 10]
    breakdown['promoters_minus_detractors'] = breakdown['promoters'] - breakdown['detractors']
    return df_clean, breakdown

def clean_join_data(df, current_month):
    """Clean and filter join cohort data and compute breakdown."""
    df['date_join_cohort_dt'] = pd.to_datetime(df['date_join_cohort'], format='%Y-%m', errors='coerce')
    df_clean = df[(df['date_join_cohort_dt'].notnull()) & (df['date_join_cohort_dt'] <= current_month)].copy()
    df_clean = df_clean[df_clean['date_join_cohort_dt'].dt.year != 1980]
    df_clean['join_cohort_month'] = df_clean['date_join_cohort_dt'].dt.to_period('M').astype(str)
    breakdown = df_clean.groupby(['join_cohort_month', 'is_multiple_renter'])\
                        .apply(calculate_nps_breakdown).reset_index()
    breakdown = breakdown[breakdown['total_responses'] >= 10]
    breakdown['promoters_minus_detractors'] = breakdown['promoters'] - breakdown['detractors']
    return df_clean, breakdown

def print_partial_table(df, table_name, columns):
    """Print the first 5 and last 5 rows of df for a given table name."""
    sys.stderr.write(f"Partial {table_name} Results:\n")
    if len(df) <= 10:
        sys.stderr.write(df[columns].to_string() + "\n\n")
    else:
        sys.stderr.write("First 5 rows:\n")
        sys.stderr.write(df[columns].head(5).to_string() + "\n")
        sys.stderr.write("...\n")
        sys.stderr.write("Last 5 rows:\n")
        sys.stderr.write(df[columns].tail(5).to_string() + "\n\n")

def save_aggregated_data(breakdown_return, breakdown_join, csv_file_return, csv_file_join, json_file):
    """Save aggregated breakdowns to CSV and JSON files."""
    breakdown_return.to_csv(csv_file_return, index=False)
    breakdown_join.to_csv(csv_file_join, index=False)
    results = {
        "nps_breakdown_by_return_month": breakdown_return.to_dict(orient="records"),
        "nps_breakdown_by_join_cohort": breakdown_join.to_dict(orient="records")
    }
    with open(json_file, "w") as jf:
        json.dump(results, jf, indent=4)
    return results

def create_visualizations(pdf_file, df_clean_return, breakdown_return, df_clean_join, breakdown_join, df, results):
    """Create all visualizations, save them to a multi-page PDF, and return a list of figures."""
    figures = []
    with PdfPages(pdf_file) as pdf:
        # Chart for Return Months with Overall Average NPS as Gray Dotted Line
        fig1, ax1 = plt.subplots(figsize=(12, 6))
        breakdown_return['plot_date'] = pd.to_datetime(breakdown_return['return_date_month'], format='%Y-%m')
        for renter_type, label in [(True, "Multiple Renters"), (False, "Single Renters")]:
            subset = breakdown_return[breakdown_return['is_multiple_renter'] == renter_type]
            ax1.plot(subset['plot_date'], subset['nps_score'], marker='o', label=label)
        # Compute overall average NPS for return months only for months with >=10 responses
        overall_return = df_clean_return.groupby('return_date_month').agg(
            total_responses=('nps_score', 'count'),
            promoters=('nps_category', lambda x: (x=='Promoter').sum()),
            detractors=('nps_category', lambda x: (x=='Detractor').sum())
        ).reset_index()
        overall_return = overall_return[overall_return['total_responses'] >= 10]
        overall_return['nps_score'] = ((overall_return['promoters'] - overall_return['detractors']) / overall_return['total_responses']) * 100
        overall_return['plot_date'] = pd.to_datetime(overall_return['return_date_month'], format='%Y-%m')
        ax1.plot(overall_return['plot_date'], overall_return['nps_score'],
                 linestyle=':', color='gray', label='Overall Average NPS')
        ax1.set_xlabel("Return Month")
        ax1.set_ylabel("NPS Score")
        ax1.set_title("NPS Score Trend Over Return Months (Cleaned & Filtered)")
        ax1.tick_params(axis='x', rotation=45)
        ax1.legend()
        ax1.grid(True)
        fig1.tight_layout()
        pdf.savefig(fig1)
        figures.append(fig1)
        
        # Chart for Join Cohorts with Overall Average NPS as Gray Dotted Line
        fig2, ax2 = plt.subplots(figsize=(12, 6))
        breakdown_join['plot_date'] = pd.to_datetime(breakdown_join['join_cohort_month'], format='%Y-%m')
        for renter_type, label in [(True, "Multiple Renters"), (False, "Single Renters")]:
            subset = breakdown_join[breakdown_join['is_multiple_renter'] == renter_type]
            ax2.plot(subset['plot_date'], subset['nps_score'], marker='o', label=label)
        overall_join = df_clean_join.groupby('join_cohort_month').agg(
            total_responses=('nps_score', 'count'),
            promoters=('nps_category', lambda x: (x=='Promoter').sum()),
            detractors=('nps_category', lambda x: (x=='Detractor').sum())
        ).reset_index()
        overall_join = overall_join[overall_join['total_responses'] >= 10]
        overall_join['nps_score'] = ((overall_join['promoters'] - overall_join['detractors']) / overall_join['total_responses']) * 100
        overall_join['plot_date'] = pd.to_datetime(overall_join['join_cohort_month'], format='%Y-%m')
        ax2.plot(overall_join['plot_date'], overall_join['nps_score'],
                 linestyle=':', color='gray', label='Overall Average NPS')
        ax2.set_xlabel("Join Cohort Month")
        ax2.set_ylabel("NPS Score")
        ax2.set_title("NPS Score Trend Over Join Cohorts (Cleaned & Filtered)")
        ax2.tick_params(axis='x', rotation=45)
        ax2.legend()
        ax2.grid(True)
        fig2.tight_layout()
        pdf.savefig(fig2)
        figures.append(fig2)
        
        # Chart for Relationship between NPS Score and Renter Type
        df['nps_score_bin'] = pd.cut(df['nps_score'], bins=range(0, 12), right=False)
        agg_counts = df.groupby('nps_score_bin')['is_multiple_renter'].agg(['sum', 'count']).reset_index()
        agg_counts.rename(columns={'sum': 'count_multiple', 'count': 'count_total'}, inplace=True)
        agg_counts['count_single'] = agg_counts['count_total'] - agg_counts['count_multiple']
        agg_counts['prop_multiple'] = agg_counts['count_multiple'] / agg_counts['count_total']
        agg_counts['nps_score_bin'] = agg_counts['nps_score_bin'].astype(str)
        
        fig3, ax3 = plt.subplots(figsize=(12, 6))
        x = np.arange(len(agg_counts))
        width = 0.35  # width of each bar
        ax3.bar(x - width/2, agg_counts['count_multiple'], width, label='Multiple Renters')
        ax3.bar(x + width/2, agg_counts['count_single'], width, label='Single Renters')
        ax3.set_xlabel("NPS Score Bin")
        ax3.set_ylabel("Number of Renters")
        ax3.set_title("Absolute Number of Renters and Proportion of Multiple Renters by NPS Score Bin")
        ax3.set_xticks(x)
        ax3.set_xticklabels(agg_counts['nps_score_bin'], rotation=45)
        ax3.legend(loc='upper left')
        ax3.grid(True, axis='y')
        
        ax3_twin = ax3.twinx()
        ax3_twin.plot(x, agg_counts['prop_multiple'], color='black', marker='o', label='Proportion Multiple')
        ax3_twin.set_ylabel("Proportion of Multiple Renters")
        lines, labels = ax3.get_legend_handles_labels()
        lines2, labels2 = ax3_twin.get_legend_handles_labels()
        ax3_twin.legend(lines + lines2, labels + labels2, loc='upper right')
        
        fig3.tight_layout()
        pdf.savefig(fig3)
        figures.append(fig3)
        
        # Chart for Relationship Between Average NPS and Proportion of Multiple Renters Over Time
        time_series = df_clean_return.groupby('return_date_month').agg(
            avg_nps=('nps_score', 'mean'),
            prop_multiple=('is_multiple_renter', 'mean'),
            total_responses=('nps_score', 'count')
        ).reset_index()
        time_series = time_series[time_series['total_responses'] >= 10]
        time_series['plot_date'] = pd.to_datetime(time_series['return_date_month'], format='%Y-%m')
        
        fig4, ax4 = plt.subplots(figsize=(12, 6))
        ax4.plot(time_series['plot_date'], time_series['avg_nps'], marker='o', color='blue', label='Average NPS')
        ax4.set_xlabel("Return Month")
        ax4.set_ylabel("Average NPS", color='blue')
        ax4.tick_params(axis='y', labelcolor='blue')
        
        ax4_twin = ax4.twinx()
        ax4_twin.plot(time_series['plot_date'], time_series['prop_multiple'], marker='s', color='red', label='Proportion Multiple Renters')
        ax4_twin.set_ylabel("Proportion of Multiple Renters", color='red')
        ax4_twin.tick_params(axis='y', labelcolor='red')
        
        ax4.set_title("Relationship Between Average NPS and Proportion of Multiple Renters Over Time")
        lines, labels = ax4.get_legend_handles_labels()
        lines2, labels2 = ax4_twin.get_legend_handles_labels()
        ax4.legend(lines + lines2, labels + labels2, loc='upper left')
        
        fig4.tight_layout()
        pdf.savefig(fig4)
        figures.append(fig4)
        
        # Detailed Time Series Analysis: Granger Causality Test
        ts_data = df_clean_return.groupby('return_date_month').agg(
            avg_nps=('nps_score', 'mean'),
            prop_multiple=('is_multiple_renter', 'mean'),
            total_responses=('nps_score', 'count')
        ).reset_index()
        ts_data = ts_data[ts_data['total_responses'] >= 10]
        ts_data['plot_date'] = pd.to_datetime(ts_data['return_date_month'], format='%Y-%m')
        ts_data = ts_data.set_index('plot_date')
        gc_test = ts.grangercausalitytests(ts_data[['prop_multiple', 'avg_nps']], maxlag=4, verbose=False)
        gc_results = {}
        for lag in range(1, 5):
            p_value = gc_test[lag][0]['ssr_chi2test'][1]
            gc_results[lag] = p_value
        
        fig5, ax5 = plt.subplots(figsize=(12, 6))
        ax5.plot(ts_data.index, ts_data['avg_nps'], label='Average NPS', color='blue')
        ax5.set_ylabel('Average NPS', color='blue')
        ax5.tick_params(axis='y', labelcolor='blue')
        ax5_twin = ax5.twinx()
        ax5_twin.plot(ts_data.index, ts_data['prop_multiple'], label='Proportion Multiple Renters', color='red')
        ax5_twin.set_ylabel('Proportion Multiple Renters', color='red')
        ax5_twin.tick_params(axis='y', labelcolor='red')
        fig5.suptitle("Detailed Time Series Analysis: Avg NPS vs. Proportion Multiple Renters")
        granger_text = "Granger Causality Test (avg_nps -> prop_multiple):\n"
        for lag, p_val in gc_results.items():
            granger_text += f"Lag {lag}: p-value = {p_val:.4f}\n"
        plt.figtext(0.5, 0.01, granger_text, wrap=True, horizontalalignment='center', fontsize=10)
        fig5.tight_layout(rect=[0, 0.05, 1, 0.95])
        pdf.savefig(fig5)
        figures.append(fig5)
        
        # Additional Analysis: Correlation and Scatter Plot between Number of Rentals and Average NPS per Customer
        user_data = df.groupby('user_ptr_id').agg(rental_count=('nps_score','count'),
                                                    avg_nps=('nps_score','mean')).reset_index()
        corr_coef = np.corrcoef(user_data['rental_count'], user_data['avg_nps'])[0,1]
        fig6, ax6 = plt.subplots(figsize=(12,6))
        ax6.scatter(user_data['rental_count'], user_data['avg_nps'], alpha=0.6)
        if len(user_data) > 1:
            slope, intercept = np.polyfit(user_data['rental_count'], user_data['avg_nps'], 1)
            x_vals = np.array([user_data['rental_count'].min(), user_data['rental_count'].max()])
            y_vals = intercept + slope * x_vals
            ax6.plot(x_vals, y_vals, color='red', linestyle='--', label=f"Trend Line (r={corr_coef:.2f})")
        ax6.set_xlabel("Number of Rentals (per user)")
        ax6.set_ylabel("Average NPS Score (per user)")
        ax6.set_title("Correlation between Number of Rentals and Average NPS Score")
        ax6.legend()
        fig6.tight_layout()
        pdf.savefig(fig6)
        figures.append(fig6)

        # Additional Analysis: Correlation and Scatter Plot between Number of Rentals and Average NPS per Customer
        user_data = df.groupby('user_ptr_id').agg(
            rental_count=('nps_score', 'count'),
            avg_nps=('nps_score', 'mean')
        ).reset_index()

        corr_coef = np.corrcoef(user_data['rental_count'], user_data['avg_nps'])[0, 1]

        fig6, ax6 = plt.subplots(figsize=(12, 6))
        ax6.scatter(user_data['rental_count'], user_data['avg_nps'], alpha=0.6)

        if len(user_data) > 1:
            slope, intercept = np.polyfit(user_data['rental_count'], user_data['avg_nps'], 1)
            x_vals = np.array([user_data['rental_count'].min(), user_data['rental_count'].max()])
            y_vals = intercept + slope * x_vals
            ax6.plot(x_vals, y_vals, color='red', linestyle='--', label=f"Trend Line (r={corr_coef:.2f})")

        # Labels and title
        ax6.set_xlabel("Number of Rentals (per user)")
        ax6.set_ylabel("Average NPS Score (per user)")
        ax6.set_title("Correlation between Number of Rentals and Average NPS Score")
        ax6.legend()

        # Annotation
        annotation_text = (
            f"Correlation Coefficient (r): {corr_coef:.2f}\n"
            + ("Positive correlation" if corr_coef > 0.1 else
            "Negative correlation" if corr_coef < -0.1 else
            "No significant correlation")
        )
        ax6.annotate(
            annotation_text,
            xy=(0.05, 0.95),
            xycoords='axes fraction',
            fontsize=10,
            backgroundcolor='white',
            verticalalignment='top',
            bbox=dict(boxstyle="round,pad=0.4", edgecolor='gray', facecolor='white')
        )

        fig6.tight_layout()
        pdf.savefig(fig6)
        figures.append(fig6)
   
        # Additional Analysis: Segmentation Analysis by NPS Category at the Customer Level
        user_seg = df.groupby('user_ptr_id').agg(
            nps_category=('nps_category', 'first'),
            is_multiple_renter=('is_multiple_renter', 'max')
        ).reset_index()
        seg = user_seg.groupby('nps_category').agg(
            total_customers=('user_ptr_id', 'count'),
            repeat_customers=('is_multiple_renter', 'sum')
        ).reset_index()
        seg['repeat_rate'] = seg['repeat_customers'] / seg['total_customers']
        fig7, ax7 = plt.subplots(figsize=(8,6))
        ax7.bar(seg['nps_category'], seg['repeat_rate'], color='skyblue')
        ax7.set_xlabel("NPS Category")
        ax7.set_ylabel("Repeat Renting Rate")
        ax7.set_title("Repeat Renting Rate by NPS Category")
        for i, rate in enumerate(seg['repeat_rate']):
            ax7.text(i, rate, f"{rate:.2f}", ha='center', va='bottom')
        fig7.tight_layout()
        pdf.savefig(fig7)
        figures.append(fig7)
        
        # Word Clouds for Each NPS Category with Key Phrases
        if 'nps_comment' in df.columns:
            categories = ['Detractor', 'Passive', 'Promoter']
            for cat in categories:
                cat_comments = df[df['nps_category'] == cat]['nps_comment'].dropna().astype(str)
                if not cat_comments.empty:
                    all_comments = " ".join(cat_comments.tolist())
                    vectorizer = CountVectorizer(ngram_range=(2, 3), stop_words='english')
                    X = vectorizer.fit_transform([all_comments])
                    freq = dict(zip(vectorizer.get_feature_names_out(), X.toarray()[0]))
                    wc = WordCloud(width=800, height=400, background_color='white')\
                         .generate_from_frequencies(freq)
                    
                    fig_wc, ax_wc = plt.subplots(figsize=(12, 6))
                    ax_wc.imshow(wc, interpolation='bilinear')
                    ax_wc.axis("off")
                    ax_wc.set_title(f"Word Cloud for {cat} Comments (Key Phrases)")
                    fig_wc.tight_layout()
                    pdf.savefig(fig_wc)
                    figures.append(fig_wc)
        else:
            sys.stderr.write("Column 'nps_comment' not found. Skipping word cloud generation.\n")
    
    return figures

# ----------------------------
# Main Execution
# ----------------------------

def main():
    # Define file paths
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    file_name = "nps_base_data_030725.csv"
    file_path = os.path.join(downloads_folder, file_name)
    
    # Define output file names
    results_folder = "results"
    os.makedirs(results_folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file_return = os.path.join(results_folder, f"nps_breakdown_by_return_month_filtered_{timestamp}.csv")
    csv_file_join = os.path.join(results_folder, f"nps_breakdown_by_join_cohort_filtered_{timestamp}.csv")
    json_file = os.path.join(results_folder, f"results_{timestamp}.json")
    pdf_file = os.path.join(results_folder, f"nps_charts_{timestamp}.pdf")
    
    # Load and preprocess data
    df = load_data(file_path)
    df = preprocess_data(df)
    
    # Set fixed current month for filtering
    current_month = pd.Timestamp('2025-03-01')
    
    # Clean and compute breakdowns for Return Months and Join Cohorts
    df_clean_return, breakdown_return = clean_return_data(df, current_month)
    df_clean_join, breakdown_join = clean_join_data(df, current_month)
    
    # Print partial table results
    print_partial_table(breakdown_return, "NPS Breakdown by Return Month", 
                          ['return_date_month', 'is_multiple_renter', 'nps_score', 'total_responses',
                           'promoters', 'passives', 'detractors', 'promoters_minus_detractors'])
    print_partial_table(breakdown_join, "NPS Breakdown by Join Cohort", 
                          ['join_cohort_month', 'is_multiple_renter', 'nps_score', 'total_responses',
                           'promoters', 'passives', 'detractors', 'promoters_minus_detractors'])
    
    # Save aggregated data and get results dictionary
    results = save_aggregated_data(breakdown_return, breakdown_join, csv_file_return, csv_file_join, json_file)
    
    # Create visualizations and save PDF
    figures = create_visualizations(pdf_file, df_clean_return, breakdown_return, df_clean_join, breakdown_join, df, results)
    
    # Display figures interactively
    for fig in figures:
        fig.show()
    plt.show()
    
    # Return JSON object for Node.js via stdout
    print(json.dumps(results))

if __name__ == "__main__":
    main()
