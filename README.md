![mortgage_analytics_flow](https://github.com/BobProphecy/images/blob/main/MortgageAnalytics.jpg?raw=true)

## Pipelines

### mortgage_analytics

The **mortgage_analytics** pipeline is designed to analyze mortgage data from two CSV files, MortgageEast.csv and MortgageWest.csv. The data is cleaned and merged, and then various calculations are performed to identify troubled mortgages. The troubled mortgages are then aggregated by state and sorted by the number of properties in trouble. Finally, the results are saved to a Delta table and a CSV file for further analysis and reporting.

### troubled_mortgage_HAF

The **troubled_mortgage_HAF** pipeline provides a mortgage assistance program that helps identify troubled mortgages in Nevada and provides assistance to customers. It reads in data from two sources, the `troubled_mortgages` table and the `MortgageCustomerNV.csv` file, and filters the customer data to only include those in Nevada. It then reformats the account number and joins the customer data with the mortgage data to create a comprehensive view of the customer's mortgage information. Finally, it writes the output to a CSV file named `mtg_assist_program.csv`. This program could be used by mortgage companies to identify and assist customers who are struggling with their mortgage payments.
## Datasets

1. **tar_troubled_mortgages**
This dataset contains information on troubled mortgages, including property ID, city, state, property type, servicer, servicer type, loan type, unpaid balance, current property value, loan status, and UPB to value ratio. This data is valuable for analyzing mortgage trends, identifying potential risks, and making informed decisions related to mortgage investments. However, the format of this dataset is not specified.

2. **myg_assist_program_csv**
This dataset contains information about the MyG Assist program, including details about the program's participants and their properties. The data includes personal information such as names, genders, ages, and contact information, as well as financial information such as income, credit ratings, and loan details. This dataset is useful for analyzing program participation and effectiveness, as well as for identifying trends and patterns in the data. It can also be used to inform decision-making related to the program and its participants.

3. **state_analysis_csv**
This dataset contains state-wise analysis of property counts and UPB (Unpaid Principal Balance) to value ratio. The data is not in any specific format and can be used for various analytical purposes, including identifying states with high property counts and evaluating the financial health of the real estate market in different regions. This dataset is particularly useful for real estate professionals, investors, and analysts who want to gain insights into the property market and make informed decisions.

4. **customers_nv**
This dataset contains information about customers, including their unique customer IDs, first and last names, gender, age, income, phone numbers, email addresses, zip codes, and credit ratings. This data is valuable for analyzing customer demographics, identifying patterns in customer behavior, and developing targeted marketing strategies. However, the format of this dataset is not specified, so further investigation is required to determine how to access and utilize this data.

5. **tar_all_mortgages**
This dataset

## Jobs

1. **job_MortgageAnalytics_AF**

The **job_MortgageAnalytics_AF** is a cloud compute job that runs on the AF fabric. This job is designed to provide valuable insights into mortgage analytics, helping us better understand the mortgage market and make informed decisions. By executing this job, we can analyze mortgage data and identify trends, enabling us to optimize our mortgage offerings and improve customer satisfaction.

2. **job_MortgageAnalytics_DBX**

The **job_MortgageAnalytics_DBX** is a cloud compute job that runs on the DBX fabric. This job is focused on analyzing mortgage data and providing insights into customer behavior. By executing this job, we can gain a deeper understanding of our customers' mortgage needs and preferences, enabling us to tailor our offerings and improve customer satisfaction. This job supports data-driven decision-making, helping us stay ahead of the competition and deliver exceptional customer experiences.