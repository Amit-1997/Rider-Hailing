
#reading all the raw data
def get_path(name):
    if name == "raw_rides":
        return "/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/events/ride_events/ride.json"
    elif name == "raw_users":
        return "/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/users/user.json"
    elif name == "raw_drivers":
        return "/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/drivers/driver.json"
    elif name == "raw_vehicles":
        return "/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/vehicles/vehicles.json"
    elif name == "raw_transactions":
        return "/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/payments/transactions/transaction.json"
    else:
        return "FIle Not Found, Please provide the correct name"