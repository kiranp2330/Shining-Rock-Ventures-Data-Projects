import pandas as pd
import time
import google_places_api 

API_KEY = 'AIzaSyAI4oQZhP02ovcc8t2nCWPGFLSuZkHOr1c' 

search_query = input("Enter the business name or search query (e.g., 'Coffee shops in Tryon, NC'): ")

all_businesses_detailed_data = []

print(f"\n--- Sending Text Search request for: '{search_query}' ---")
data_text_search = google_places_api.text_search_places(search_query, API_KEY)

print("\n--- Text Search API Response Status ---")
print(f"Status: {data_text_search.get('status')}")

if data_text_search.get('status') == 'OK' and data_text_search.get('results'):
    found_count = len(data_text_search['results'])
    print(f"Found {found_count} potential matches for '{search_query}'.")

    num_results_to_process = min(found_count, 5) 
    print(f"Fetching detailed information for the top {num_results_to_process} matches...")

    for i, basic_match in enumerate(data_text_search['results'][:num_results_to_process]):
        place_id = basic_match.get('place_id')
        if not place_id:
            print(f" Skipping result {i+1} due to missing Place ID.")
            continue

        print(f"\n--- Fetching details for match {i+1}: {basic_match.get('name', 'N/A')} (Place ID: {place_id}) ---")

        data_place_details = google_places_api.get_place_details(place_id, API_KEY)

        if data_place_details.get('status') == 'OK' and 'result' in data_place_details:
            detailed_place_info = data_place_details['result']
            
            business_dict = {
                'Name': detailed_place_info.get('name', 'N/A'),
                'Place ID': detailed_place_info.get('place_id', 'N/A'),
                'Address': detailed_place_info.get('formatted_address', 'N/A'),
                'Phone': detailed_place_info.get('formatted_phone_number', 'N/A'),
                'Website': detailed_place_info.get('website', 'N/A'),
                'Rating': detailed_place_info.get('rating', 'N/A'),
                'Total Ratings': detailed_place_info.get('user_ratings_total', 'N/A'),
                'Business Status': detailed_place_info.get('business_status', 'N/A'),
                'Types': ', '.join(detailed_place_info.get('types', [])),
                'Latitude': 'N/A',
                'Longitude': 'N/A'
            }
            
            if 'geometry' in detailed_place_info and 'location' in detailed_place_info['geometry']:
                business_dict['Latitude'] = detailed_place_info['geometry']['location'].get('lat', 'N/A')
                business_dict['Longitude'] = detailed_place_info['geometry']['location'].get('lng', 'N/A')

            all_businesses_detailed_data.append(business_dict)

            print(f" Name: {business_dict['Name']}")
            print(f" Address: {business_dict['Address']}")
            print(f" Phone: {business_dict['Phone']}")
            print(f" Website: {business_dict['Website']}")
            print(f" Rating: {business_dict['Rating']} (Total Ratings: {business_dict['Total Ratings']})")
            print(f" Business Status: {business_dict['Business Status']}")
            print(f" Types: {business_dict['Types']}")
            print(f" Latitude: {business_dict['Latitude']}, Longitude: {business_dict['Longitude']}")

        else:
            print(f" No detailed results or error for Place ID {place_id}: {data_place_details.get('error_message', 'No specifc error message.')}")

        # delay to be gentle on the API
        time.sleep(0.1) 

elif data_text_search.get('status') == 'ZERO_RESULTS':
    print("No results found for your query.")
else:
    print(f"An error occurred during Text Search: {data_text_search.get('error_message', 'No specific error message')}")

if all_businesses_detailed_data:
    df_all_businesses = pd.DataFrame(all_businesses_detailed_data)
    print("\n--- Consolidated Pandas DataFrame Created ---")
    print(df_all_businesses.head(num_results_to_process if 'num_results_to_process' in locals() else 5))
    print(f"\nDataFrame shape: {df_all_businesses.shape}")

    output_filename = "google_places_multiple_results_data.csv"
    df_all_businesses.to_csv(output_filename, index=False)
    print(f"\n--- All collected data saved to '{output_filename}' ---")
else:
    print("\nNo business data collected to create a DataFrame or save to CSV.")

print("\nScript finished.")
