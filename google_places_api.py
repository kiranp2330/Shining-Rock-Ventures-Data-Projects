import requests
import json
import time

# Text Search API
BASE_URL_TEXT_SEARCH = "https://maps.googleapis.com/maps/api/place/textsearch/json"

# Place Details API
BASE_URL_PLACE_DETAILS = "https://maps.googleapis.com/maps/api/place/details/json"

DEFAULT_PLACE_DETAILS_FIELDS = "name,formatted_address,geometry,rating,user_ratings_total,website,formatted_phone_number,business_status,place_id,type"

def text_search_places(query: str, api_key: str, **kwargs) -> dict:
    """
    Performs a text search for places using the Google Places Text Search API.

    Args:
        query (str): The text string on which to search, e.g., 'restaurants in Sydney'.
        api_key (str): Your Google Cloud API Key with Places API enabled.
        **kwargs: Additional parameters for the API request (e.g., 'location', 'radius').
                

    Returns:
        dict: The JSON response from the Text Search API. Returns an empty dict
              or a dict with 'error' key if the request fails.
    """
    print(f"Calling Text Search API for query: '{query}'")
    params = {
        'query': query,
        'key': api_key,
    }
    params.update(kwargs) 

    try:
        response = requests.get(BASE_URL_TEXT_SEARCH, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred during Text Search: {err}")
        return {"error": str(err), "status": "HTTP_ERROR"}
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error occurred during Text Search: {err}")
        return {"error": str(err), "status": "CONNECTION_ERROR"}
    except requests.exceptions.Timeout as err:
        print(f"Timeout error occurred during Text Search: {err}")
        return {"error": str(err), "status": "TIMEOUT_ERROR"}
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred during Text Search: {err}")
        return {"error": str(err), "status": "REQUEST_ERROR"}
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON response from Text Search. Raw response: {response.text}")
        return {"error": "JSON_DECODE_ERROR", "status": "JSON_ERROR"}


def get_place_details(place_id: str, api_key: str, fields: str = None, **kwargs) -> dict:
    """
    Retrieves detailed information about a specific place using the Google Places Details API.

    Args:
        place_id (str): The unique identifier of the place for which to return details.
        api_key (str): Your Google Cloud API Key with Places API enabled.
        fields (str, optional): A comma-separated list of fields to return.
                                If None, uses DEFAULT_PLACE_DETAILS_FIELDS.
                                See Google Places Details API documentation for available fields.
        **kwargs: Additional parameters for the API request (e.g., 'sessiontoken').
                  

    Returns:
        dict: The JSON response from the Place Details API. Returns an empty dict
              or a dict with 'error' key if the request fails.
    """
    print(f"Calling Place Details API for Place ID: '{place_id}'")
    params = {
        'place_id': place_id,
        'key': api_key,
        'fields': fields if fields else DEFAULT_PLACE_DETAILS_FIELDS,
    }
    params.update(kwargs) 

    try:
        response = requests.get(BASE_URL_PLACE_DETAILS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred during Place Details: {err}")
        return {"error": str(err), "status": "HTTP_ERROR"}
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error occurred during Place Details: {err}")
        return {"error": str(err), "status": "CONNECTION_ERROR"}
    except requests.exceptions.Timeout as err:
        print(f"Timeout error occurred during Place Details: {err}")
        return {"error": str(err), "status": "TIMEOUT_ERROR"}
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred during Place Details: {err}")
        return {"error": str(err), "status": "REQUEST_ERROR"}
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON response from Place Details. Raw response: {response.text}")
        return {"error": "JSON_DECODE_ERROR", "status": "JSON_ERROR"}


# test
if __name__ == "__main__":
    MY_API_KEY = "AIzaSyAI4oQZhP02ovcc8t2nCWPGFLSuZkHOr1c" 

    print("--- Testing Google Places API Functions ---")

    # --- Text Search ---
    search_query = "Starbucks Main St, New York"
    text_search_response = text_search_places(search_query, MY_API_KEY)

    if text_search_response.get("status") == "OK" and text_search_response.get("results"):
        first_result = text_search_response["results"][0]
        place_id_from_search = first_result.get("place_id")
        print(f"\nText Search successful. First result: {first_result.get('name')}, Place ID: {place_id_from_search}")

        # --- Place Details ---
        if place_id_from_search:
            print("\n--- Fetching Place Details ---")
            place_details_response = get_place_details(place_id_from_search, MY_API_KEY)

            if place_details_response.get("status") == "OK" and "result" in place_details_response:
                details = place_details_response["result"]
                print(f"Name: {details.get('name')}")
                print(f"Address: {details.get('formatted_address')}")
                print(f"Phone: {details.get('formatted_phone_number')}")
                print(f"Rating: {details.get('rating')}")
                print(f"Website: {details.get('website')}")
                print(f"Latitude: {details.get('geometry', {}).get('location', {}).get('lat')}")
                print(f"Longitude: {details.get('geometry', {}).get('location', {}).get('lng')}")
            else:
                print(f"Place Details failed: {place_details_response.get('error', place_details_response.get('status'))}")
        else:
            print("No Place ID found from text search to fetch details.")
    else:
        print(f"Text Search failed: {text_search_response.get('error', text_search_response.get('status'))}")

    print("\n--- Testing complete ---")