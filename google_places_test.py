import pandas as pd
import google_places_api 

API_KEY = 'API-KEY' 

search_query = input("Enter the business name or search query (e.g., 'Starbucks Main St'): ")

all_business_data = []

print(f"--- Sending request for: '{search_query}'---")
data_text_search = google_places_api.text_search_places(search_query, API_KEY)

print("\n--- API Response Status ---")
print(f"Status: {data_text_search.get('status')}")

best_match_place_id = None
if data_text_search.get('status') == 'OK' and data_text_search.get('results'):
    print(f"Found {len(data_text_search['results'])} potential matches for '{search_query}':")

    best_match = data_text_search['results'][0]
    best_match_place_id = best_match.get('place_id')

    print("\n--- Identified Best Match (first result) ---")
    print(f" Name: {best_match.get('name', 'N/A')}")
    print(f" Place ID: {best_match_place_id}")

elif data_text_search.get('status') == 'ZERO_RESULTS': 
    print('No results found for your query')
else:
    print(f"An error occurred: {data_text_search.get('error_message', 'No specific error message.')}")

if best_match_place_id:
    print(f"\n--- Proceeding to fetch detailed information for Place ID: {best_match_place_id} ---")

    data_place_details = google_places_api.get_place_details(best_match_place_id, API_KEY)

    if data_place_details.get('status') == 'OK' and 'result' in data_place_details:
        detailed_place_info = data_place_details['result']
        
        business_dict = {
            'Name': detailed_place_info.get('name', 'N/A'),
            'Place ID': detailed_place_info.get('place_id', 'N/A'),
            'Address': detailed_place_info.get('formatted_address', 'N/A'),
            'Phone': detailed_place_info.get('formatted_phone_number', 'N/A'),
            'Website': detailed_place_info.get('website', 'N/A'), 
            'Business Status': detailed_place_info.get('business_status', 'N/A'),
            'Types': ', '.join(detailed_place_info.get('types', [])),
            'Latitude': 'N/A',
            'Longitude': 'N/A'
        }
        
        if 'geometry' in detailed_place_info and 'location' in detailed_place_info['geometry']:
            business_dict['Latitude'] = detailed_place_info['geometry']['location'].get('lat', 'N/A')
            business_dict['Longitude'] = detailed_place_info['geometry']['location'].get('lng', 'N/A')

        all_business_data.append(business_dict)

        print(f" Name: {business_dict['Name']}")
        print(f" Address: {business_dict['Address']}")
        print(f" Phone: {business_dict['Phone']}")
        print(f" Website: {business_dict['Website']}")
        print(f" Business Status: {business_dict['Business Status']}")
        print(f" Types: {business_dict['Types']}")
        print(f" Latitude: {business_dict['Latitude']}, Longitude: {business_dict['Longitude']}")
    else: 
        print(f"No detailed results or error for Place ID {best_match_place_id}: {data_place_details.get('error_message', 'No specific error message.')}")
else: 
    print("\nNo best match found to fetch further details.")

if all_business_data:
    df = pd.DataFrame(all_business_data)
    print("\n--- Pandas DataFrame Created ---")
    print(df.head())
    print(f"\nDataFrame shape: {df.shape}")

    output_filename = "google_places_data.csv"
    df.to_csv(output_filename, index = False)
    print(f"\n--- Data saved to '{output_filename}' ---")
else:
    print("\nNo business data collected to create a DataFrame or save to CSV.")

print("\nScript finished.")
