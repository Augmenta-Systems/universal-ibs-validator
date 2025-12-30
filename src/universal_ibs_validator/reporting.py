import pandas as pd
from datetime import datetime

def generate_html_report(failures: pd.DataFrame, output_file: str = "ibs_validation_report.html"):
    if failures.empty:
        html_content = "<html><body><h1>No Data Consistency Errors Found</h1></body></html>"
    else:
        # Create a summary table
        summary = failures.groupby(['RULE_ID', 'DESCRIPTION', 'OPERATOR']).size().reset_index(name='Fail Count')
        
        # Format the detailed table
        # Move key columns to the front
        cols = ['RULE_ID', 'LHS_VALUE', 'OPERATOR', 'RHS_VALUE', 'DIFF', 'DESCRIPTION']
        remaining = [c for c in failures.columns if c not in cols and c != 'IS_FAIL']
        failures = failures[cols + remaining]

        # Generate HTML with CSS
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>IBS Validation Report</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f4f4f9; }}
                h1 {{ color: #2c3e50; }}
                .container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }}
                th {{ background-color: #34495e; color: white; padding: 10px; text-align: left; }}
                td {{ border-bottom: 1px solid #ddd; padding: 8px; }}
                tr:hover {{ background-color: #f1f1f1; }}
                .fail-negative {{ color: #e74c3c; font-weight: bold; }}
                .fail-positive {{ color: #e67e22; font-weight: bold; }}
                .summary-box {{ margin-bottom: 30px; border-left: 5px solid #e74c3c; padding-left: 15px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Universal IBS Validator Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="summary-box">
                    <h2>Executive Summary</h2>
                    {summary.to_html(index=False, border=0, classes="table")}
                </div>

                <h2>Detailed Failures</h2>
                <p>The table below shows every data point where the validation logic failed.</p>
                {failures.to_html(index=False, border=0, classes="table", float_format=lambda x: '%.3f' % x)}
            </div>
        </body>
        </html>
        """

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"Report generated: {output_file}")
