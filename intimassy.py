import sqlite3

# Path to the SQLite database
DB_PATH = 'ascdatabase2.db'

def fetch_entries_with_details(cursor):
    # Fetch entries and related details
    cursor.execute("SELECT * FROM entries;")
    entries = cursor.fetchall()
    
    cursor.execute("SELECT * FROM entry_partner;")
    entry_partner = cursor.fetchall()

    cursor.execute("SELECT * FROM entry_sex_type;")
    entry_sex_type = cursor.fetchall()

    cursor.execute("SELECT * FROM entry_position;")
    entry_position = cursor.fetchall()

    cursor.execute("SELECT * FROM entry_place;")
    entry_place = cursor.fetchall()

    cursor.execute("SELECT * FROM partners;")
    partners = cursor.fetchall()

    cursor.execute("SELECT * FROM positions;")
    positions = cursor.fetchall()

    # Remove fetch for places if the table does not exist
    # cursor.execute("SELECT * FROM places;")
    # places = cursor.fetchall()

    sex_types = {
        0: 'Vaginal',
        1: 'Oral',
        2: 'Handjob',
        3: 'Masturbation',
        4: 'Finger',
        5: 'Toy',
        6: 'Anal',
        7: 'Group',
        8: 'Active',
        9: 'Passive',
        10: 'BDSM'
    }

    # Helper functions
    def get_partner_name(partner_id):
        for partner in partners:
            print(partner)
            if partner[0] == partner_id:
                return partner[2]
        return 'Unknown'

    def get_position_name(position_id):
        for position in positions:
            if position[0] == position_id:
                return position[2]
        return 'Unknown'

    def get_place_name(place_id):
        for entry in entry_place:
            if entry[0] == place_id:
                return entry[2]
        return 'Unknown'

    # Process and print each entry
    for entry in entries:
        entry_id, user_id, date, duration, note, rating, initiator, safety_status, total_org, total_org_partner = entry
        
        # Get related details
        partners_involved = [get_partner_name(partner_id) for entry_id_, partner_id in entry_partner if entry_id_ == entry_id]
        sex_types_involved = [sex_types.get(sex_type_id, 'Unknown') for entry_id_, sex_type_id in entry_sex_type if entry_id_ == entry_id]
        positions_involved = [get_position_name(position_id) for entry_id_, position_id in entry_position if entry_id_ == entry_id]
        places_involved = [get_place_name(place_id) for entry_id_, place_id in entry_place if entry_id_ == entry_id]

        # Print entry details
        print(f"Entry ID: {entry_id}")
        print(f"Date: {date}")
        print(f"Duration: {duration} minutes")
        print(f"Notes: {note}")
        print(f"Rating: {rating}")
        print(f"Activity Initiator: {get_partner_name(initiator)}")
        print(f"Number of Orgasms: {total_org}")
        print(f"Partner's Orgasm Count: {total_org_partner}")

        print("Partners Involved:")
        for partner in partners_involved:
            print(f"  - {partner}")

        print("Sex Types Involved:")
        for sex_type in sex_types_involved:
            print(f"  - {sex_type}")

        print("Positions Involved:")
        for position in positions_involved:
            print(f"  - {position}")

        print("Places Involved:")
        for place in places_involved:
            print(f"  - {place}")

        print("=" * 50)

def main():
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Fetch and display entry details
    fetch_entries_with_details(cursor)
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
