import os
import pandas as pd
import requests
import time
import logging
import csv

# Define the structure for data collection
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("processing.log"),
                        logging.StreamHandler()
                    ])


def create_property_dict():
    return {
        "address": None,
        "city": None,
        "state": None,
        "zipcode": None,
        "county": None,
        "countyFIPS": None,
        "latitude": None,
        "longitude": None,
        "bathrooms": None,
        "bathroomsFull": None,
        "bathroomsHalf": None,
        "bedrooms": None,
        "homeStatus": None,
        "homeType": None,
        "livingArea": None,
        "lotSize": None,
        "price": None,
        "currency": None,
        "description": None,
        "contingentListingType": None,
        "datePostedString": None,
        "datePriceChanged": None,
        "dateSoldString": None,
        "daysOnZillow": None,
        "favoriteCount": None,
        "isListedByOwner": None,
        "isNonOwnerOccupied": None,
        "isPreforeclosureAuction": None,
        "isBankOwned": None,
        "isForeclosure": None,
        "isFSBA": None,
        "isFSBO": None,
        "isComingSoon": None,
        "isForAuction": None,
        "isNewHome": None,
        "isOpenHouse": None,
        "isPending": None,
        "isZillowOwned": None,
        "keystoneHomeStatus": None,
        "lastSoldPrice": None,
        "listingTypeDimension": None,
        "listing_agent": None,
        "livingAreaUnits": None,
        "livingAreaUnitsShort": None,
        "livingAreaValue": None,
        "lotAreaUnits": None,
        "lotAreaValue": None,
        "taxAssessedValue": None,
        "taxAssessedYear": None,
        "taxHistory": None,
        "appliances": None,
        "architecturalStyle": None,
        "heating": None,
        "cooling": None,
        "fencing": None,
        "flooring": None,
        "basement": None,
        "yearBuilt": None,
        "laundryFeatures": None,
        "levels": None,
        "stories": None,
        "parkingFeatures": None,
        "structureType": None,
        "thumb_url": None,
        "tourEligibility": None,
        "tourPhotos": None,
        "timeOnZillow": None,
        "timeZone": None,
        "virtualTourUrl": None,
        "zipcodeSearchUrl": None,
        "zestimate": None,
        "rentZestimate": None,
        "hugePhotos": [],
        "agentEmail": None,
        "agentLicenseNumber": None,
        "agentName": None,
        "agentPhoneNumber": None,
        "attributionTitle": None,
        "brokerName": None,
        "brokerPhoneNumber": None,
        "buyerAgentName": None,
        "buyerBrokerageName": None
    }

# Function to handle data extraction from a single property


def extract_property_data(property_data):
    property_dict = create_property_dict()
    property_dict["address"] = property_data.get("abbreviatedAddress")
    property_dict["city"] = property_data.get("city")
    property_dict["state"] = property_data.get("state")
    property_dict["zipcode"] = property_data.get("zipcode")
    property_dict["county"] = property_data.get("county")
    property_dict["countyFIPS"] = property_data.get("countyFIPS")
    property_dict["latitude"] = property_data.get("latitude")
    property_dict["longitude"] = property_data.get("longitude")
    property_dict["bathrooms"] = property_data.get("bathrooms")
    property_dict["bathroomsFull"] = property_data.get("bathroomsFull")
    property_dict["bathroomsHalf"] = property_data.get("bathroomsHalf")
    property_dict["bedrooms"] = property_data.get("bedrooms")
    property_dict["homeStatus"] = property_data.get("homeStatus")
    property_dict["homeType"] = property_data.get("homeType")
    property_dict["livingArea"] = property_data.get("livingArea")
    property_dict["lotSize"] = property_data.get("lotSize")
    property_dict["price"] = property_data.get("price")
    property_dict["currency"] = property_data.get("currency")
    property_dict["description"] = property_data.get("description")
    property_dict["contingentListingType"] = property_data.get(
        "contingentListingType")
    property_dict["datePostedString"] = property_data.get("datePostedString")
    property_dict["datePriceChanged"] = property_data.get("datePriceChanged")
    property_dict["dateSoldString"] = property_data.get("dateSoldString")
    property_dict["daysOnZillow"] = property_data.get("daysOnZillow")
    property_dict["favoriteCount"] = property_data.get("favoriteCount")
    property_dict["isListedByOwner"] = property_data.get("isListedByOwner")
    property_dict["isNonOwnerOccupied"] = property_data.get(
        "isNonOwnerOccupied")
    property_dict["isPreforeclosureAuction"] = property_data.get(
        "isPreforeclosureAuction")
    property_dict["isBankOwned"] = property_data.get("isBankOwned")
    property_dict["isForeclosure"] = property_data.get("isForeclosure")
    property_dict["isFSBA"] = property_data.get("isFSBA")
    property_dict["isFSBO"] = property_data.get("isFSBO")
    property_dict["isComingSoon"] = property_data.get("isComingSoon")
    property_dict["isForAuction"] = property_data.get("isForAuction")
    property_dict["isNewHome"] = property_data.get("isNewHome")
    property_dict["isOpenHouse"] = property_data.get("isOpenHouse")
    property_dict["isPending"] = property_data.get("isPending")
    property_dict["isZillowOwned"] = property_data.get("isZillowOwned")
    property_dict["keystoneHomeStatus"] = property_data.get(
        "keystoneHomeStatus")
    property_dict["lastSoldPrice"] = property_data.get("lastSoldPrice")
    property_dict["listingTypeDimension"] = property_data.get(
        "listingTypeDimension")
    property_dict["listing_agent"] = property_data.get("listing_agent")
    property_dict["livingAreaUnits"] = property_data.get("livingAreaUnits")
    property_dict["livingAreaUnitsShort"] = property_data.get(
        "livingAreaUnitsShort")
    property_dict["livingAreaValue"] = property_data.get("livingAreaValue")
    property_dict["lotAreaUnits"] = property_data.get("lotAreaUnits")
    property_dict["lotAreaValue"] = property_data.get("lotAreaValue")
    property_dict["taxAssessedValue"] = property_data.get("taxAssessedValue")
    property_dict["taxAssessedYear"] = property_data.get("taxAssessedYear")
    property_dict["taxHistory"] = property_data.get("taxHistory")
    property_dict["appliances"] = property_data.get("appliances")
    property_dict["architecturalStyle"] = property_data.get(
        "architecturalStyle")
    property_dict["heating"] = property_data.get("heating")
    property_dict["cooling"] = property_data.get("cooling")
    property_dict["fencing"] = property_data.get("fencing")
    property_dict["flooring"] = property_data.get("flooring")
    property_dict["basement"] = property_data.get("basement")
    property_dict["yearBuilt"] = property_data.get("yearBuilt")
    property_dict["laundryFeatures"] = property_data.get("laundryFeatures")
    property_dict["levels"] = property_data.get("levels")
    property_dict["stories"] = property_data.get("stories")
    property_dict["parkingFeatures"] = property_data.get("parkingFeatures")
    property_dict["structureType"] = property_data.get("structureType")
    property_dict["thumb_url"] = property_data.get("thumb_url")
    property_dict["tourEligibility"] = property_data.get("tourEligibility")
    property_dict["tourPhotos"] = property_data.get("tourPhotos")
    property_dict["timeOnZillow"] = property_data.get("timeOnZillow")
    property_dict["timeZone"] = property_data.get("timeZone")
    property_dict["virtualTourUrl"] = property_data.get("virtualTourUrl")
    property_dict["zipcodeSearchUrl"] = property_data.get("zipcodeSearchUrl")
    property_dict["zestimate"] = property_data.get("zestimate")
    property_dict["rentZestimate"] = property_data.get("rentZestimate")
    property_dict["hugePhotos"] = property_data.get("hugePhotos", [])
    property_dict["agentEmail"] = property_data.get("agentEmail")
    property_dict["agentLicenseNumber"] = property_data.get(
        "agentLicenseNumber")
    property_dict["agentName"] = property_data.get("agentName")
    property_dict["agentPhoneNumber"] = property_data.get("agentPhoneNumber")
    property_dict["attributionTitle"] = property_data.get("attributionTitle")
    property_dict["brokerName"] = property_data.get("brokerName")
    property_dict["brokerPhoneNumber"] = property_data.get("brokerPhoneNumber")
    property_dict["buyerAgentName"] = property_data.get("buyerAgentName")
    property_dict["buyerBrokerageName"] = property_data.get(
        "buyerBrokerageName")
    return property_dict

# Function to download and save images


def download_images(image_urls, address):
    # Create a directory-safe version of the address
    dir_name = address.replace(" ", "_")
    directory = f"pictures/{dir_name}"

    # Create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download each image
    for i, image_data in enumerate(image_urls):
        # Extract the URL from the image data dictionary
        url = image_data.get("url")
        if url:
            # Correctly construct the image file path
            image_path = os.path.join(directory, f"pic{i+1}_{dir_name}.jpg")
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    with open(image_path, 'wb') as file:
                        file.write(response.content)
                else:
                    print(f"Error downloading image {
                        url}: HTTP Status Code {response.status_code}")
            except Exception as e:
                print(f"Error downloading image {url}: {e}")
        else:
            print(f"No URL found in image data: {image_data}")

# Function to save data to CSV


def save_to_csv(row, filename, index):
    # Append a row to a CSV file
    with open(filename, 'a', newline='') as csvfile:
        fieldnames = ['index'] + list(row.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()
        row['index'] = index
        writer.writerow(row)


def get_property_data(address):
    url = "https://zillow56.p.rapidapi.com/search_address"
    querystring = {"address": address}

    headers = {
        "x-rapidapi-key": "1c0f0f8622msh8b446348de61d90p169e16jsn9812bfb8c213",
        "x-rapidapi-host": "zillow56.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {
            address}: HTTP Status Code {response.status_code}")
        return None


# Main script


def main():
    # Load the DataFrame
    df = pd.read_csv("full_address_2Y_sold.csv")
    chunk_size = 1000

    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        chunk = df[start:end]
        start_time = time.time()

        for index, row in chunk.iterrows():
            try:
                address = row["Full_Address"]
                property_data = get_property_data(address)

                if property_data:
                    property_dict = extract_property_data(property_data)

                    if property_dict.get("address") and property_dict.get("hugePhotos"):
                        download_images(
                            property_dict["hugePhotos"], property_dict["address"])

                    property_dict.pop("hugePhotos", None)
                    save_to_csv(property_dict,
                                "all_sold_property_data.csv", row.name)
                    logging.info(f"Finished: {property_dict['address']}")

                    if index % 50 == 0:
                        logging.info(f"Finished downloading {
                                    index} properties.")
            except Exception as e:
                logging.error(f"Error processing row {index}: {e}")

        end_time = time.time()
        length = round((end_time - start_time) / 60, 2)
        logging.info(f"Finished processing chunk in {length} minutes")


if __name__ == "__main__":
    main()
