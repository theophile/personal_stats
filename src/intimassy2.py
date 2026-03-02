import sqlite3
import re
from datetime import datetime

class ASCDatabase:
    def __init__(self, db_path):
        """Initializes the connection to the SQLite database."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

        # Predefined mapping for sex types
        self.sex_type_mapping = {
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

        # Initiator mapping
        self.initiator_mapping = {
            0: 'Spontaneously',
            1: 'Me',
            2: 'My Partner',
            3: 'Both of Us'
        }

        self.place_mapping = {
            0: 'Bedroom',
            1: 'Kitchen',
            2: 'Shower',
            3: 'Restroom',
            4: 'Living Room',
            5: 'Garage',
            6: 'Backyard',
            7: 'Roof',
            8: 'Jacuzzi',
            9: 'Pool',
            10: 'Beach',
            11: 'Home',
            12: 'Hotel',
            13: 'Lifestyle Club',
            14: 'Cinema',
            15: 'Theatre',
            16: 'School',
            17: 'Museum',
            18: 'Car',
            19: 'Plane',
            20: 'Train',
            21: 'Ship',
            22: 'Public'
        }

    def connect(self):
        """Connect to the database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print("Database connected successfully.")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False
        return True

    def close(self):
        """Close the connection to the database."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    # Example to fetch all entries from the entries table
    def fetch_all_entries(self):
        """Fetch all entries from the 'entries' table."""
        query = "SELECT * FROM entries"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Fetch entry by ID
    def fetch_entry_by_id(self, entry_id):
        """Fetch an entry by its ID and parse integer values."""
        query = "SELECT * FROM entries WHERE entry_id = ?"
        self.cursor.execute(query, (entry_id,))
        entry = self.cursor.fetchone()

        if entry:
            # Parsing the last five integer values based on the structure you provided
            rating = entry[5]
            initiator = self.initiator_mapping.get(entry[6], 'Unknown')
            protection_used = 'Yes' if entry[7] == 1 else 'No'
            orgasms = entry[8]
            partner_orgasms = entry[9]

            parsed_entry = {
                'entry_id': entry[0],
                'user_id': entry[1],
                'date': entry[2],
                'duration': entry[3],
                'note': entry[4],
                'rating': rating,
                'initiator': initiator,
                'protection_used': protection_used,
                'orgasms': orgasms,
                'partner_orgasms': partner_orgasms
            }

            return parsed_entry
        else:
            return None

    # Fetch places associated with an entry
    def fetch_entry_places(self, entry_id):
        """Fetch the places associated with an entry and map them to place names."""
        query = "SELECT place_id FROM entry_place WHERE entry_id = ?"
        self.cursor.execute(query, (entry_id,))
        places = self.cursor.fetchall()

        # Map place IDs to their names
        place_names = [self.place_mapping.get(place[0], 'Unknown Place') for place in places]

        return place_names

    # Fetch sex types associated with an entry
    def fetch_entry_sex_types(self, entry_id):
        """Fetch sex types associated with a specific entry and map to their names."""
        query = """
        SELECT sex_type_id 
        FROM entry_sex_type 
        WHERE entry_id = ?
        """
        self.cursor.execute(query, (entry_id,))
        sex_type_ids = self.cursor.fetchall()
        return [self.sex_type_mapping[sex_type_id[0]] for sex_type_id in sex_type_ids]

    # Fetch positions associated with an entry
#    def fetch_entry_positions(self, entry_id):
#        """Fetch positions associated with a specific entry."""
#        query = """
#        SELECT positions.name 
#        FROM entry_position 
#        JOIN positions ON entry_position.position_id = positions.position_id
#        WHERE entry_position.entry_id = ?
#        """
#        self.cursor.execute(query, (entry_id,))
#        return self.cursor.fetchall()

    def fetch_entry_position_ids(self, entry_id):
        """Fetch position IDs associated with a specific entry."""
        query = """
        SELECT entry_position.position_id
        FROM entry_position
        WHERE entry_position.entry_id = ?
        """
        self.cursor.execute(query, (entry_id,))
        return [row[0] for row in self.cursor.fetchall()]  # Return a list of position IDs

    # Fetch the name of a position given its ID
    def fetch_position_name(self, position_id):
        """Fetch the name of a position based on its ID."""
        query = """
        SELECT name
        FROM positions
        WHERE position_id = ?
        """
        self.cursor.execute(query, (position_id,))
        result = self.cursor.fetchone()

        if result:
            position_name = result[0]
            # If the position_id is 6, clean the name
            if position_id == 6:
                position_name = re.sub(r'\(.*?\)', '', position_name)  # Remove content within parentheses
                position_name = position_name.strip()  # Remove trailing whitespace
            return position_name
        return None  # Return None if no result is found

    # Fetch entry with all associated details (places, sex types, positions)
    def fetch_entry_with_details(self, entry_id):
        """Fetch an entry along with its associated places, sex types, and positions, with parsed entry details."""
        # Fetch and parse the entry
        entry = self.fetch_entry_by_id(entry_id)
        if not entry:
            print(f"Entry {entry_id} not found.")
            return None

        # Fetch associated places and map to names
        places = self.fetch_entry_places(entry_id)

        # Fetch associated sex types (mapped to names)
        sex_types = self.fetch_entry_sex_types(entry_id)

        # Fetch associated positions
        position_ids = self.fetch_entry_position_ids(entry_id)
        positions = []
        for position in position_ids:
            positions.append(self.fetch_position_name(position))
        #positions = [position[0] for position in position_ids]  # Extract position IDs

        # Combine all the details into a dictionary
        entry_details = {
            'entry': entry,
            'places': places,
            'sex_types': sex_types,
            'positions': positions,
            'position_ids': position_ids
        }

        return entry_details

    # Add an entry_place record
    def add_entry_place(self, entry_id, place_id):
        """Add a place to an entry in the 'entry_place' table."""
        query = "INSERT INTO entry_place (entry_id, place_id) VALUES (?, ?)"
        try:
            self.cursor.execute(query, (entry_id, place_id))
            self.conn.commit()
            print("Place for entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding place: {e}")
            return False
        return True

    # Add an entry_sex_type record
    def add_entry_sex_type(self, entry_id, sex_type_id):
        """Add a new sex type to an entry in the 'entry_sex_type' table."""
        query = "INSERT INTO entry_sex_type (entry_id, sex_type_id) VALUES (?, ?)"
        try:
            self.cursor.execute(query, (entry_id, sex_type_id))
            self.conn.commit()
            print("Sex type for entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding sex type: {e}")
            return False
        return True

    # Add an entry_position record
    def add_entry_position(self, entry_id, position_id):
        """Add a position to an entry in the 'entry_position' table."""
        query = "INSERT INTO entry_position (entry_id, position_id) VALUES (?, ?)"
        try:
            self.cursor.execute(query, (entry_id, position_id))
            self.conn.commit()
            print("Position for entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding position: {e}")
            return False
        return True

    # Fetch entries by user_id
    def fetch_entries_by_user(self, user_id):
        """Fetch all entries for a specific user."""
        query = "SELECT * FROM entries WHERE user_id = ?"
        self.cursor.execute(query, (user_id,))
        return self.cursor.fetchall()

    # Add a new entry to the 'entries' table
    def add_entry(self, entry_data):
        """Add a new entry to the 'entries' table."""
        query = """
        INSERT INTO entries (user_id, date, duration, note, rating, initiator, safety_status, total_org, total_org_partner)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            self.cursor.execute(query, entry_data)
            self.conn.commit()
            print("Entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding entry: {e}")
            return False
        return True

    # Update an existing entry
    def update_entry(self, entry_id, updated_data):
        """Update an existing entry by its ID."""
        query = """
        UPDATE entries
        SET user_id = ?, date = ?, duration = ?, note = ?, rating = ?, initiator = ?, safety_status = ?, total_org = ?, total_org_partner = ?
        WHERE entry_id = ?
        """
        try:
            self.cursor.execute(query, (*updated_data, entry_id))
            self.conn.commit()
            print(f"Entry {entry_id} updated successfully.")
        except sqlite3.Error as e:
            print(f"Error updating entry: {e}")
            return False
        return True

    # Delete an entry by its ID
    def delete_entry(self, entry_id):
        """Delete an entry by its ID."""
        query = "DELETE FROM entries WHERE entry_id = ?"
        try:
            self.cursor.execute(query, (entry_id,))
            self.conn.commit()
            print(f"Entry {entry_id} deleted successfully.")
        except sqlite3.Error as e:
            print(f"Error deleting entry: {e}")
            return False
        return True

    # Fetch all partners
    def fetch_all_partners(self):
        """Fetch all partners from the 'partners' table."""
        query = "SELECT * FROM partners"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Fetch all positions
    def fetch_all_positions(self):
        """Fetch all positions from the 'positions' table."""
        query = "SELECT * FROM positions"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Add an entry_sex_type record
    def add_entry_sex_type(self, entry_id, sex_type_id):
        """Add a new record to the 'entry_sex_type' table."""
        query = "INSERT INTO entry_sex_type (entry_id, sex_type_id) VALUES (?, ?)"
        try:
            self.cursor.execute(query, (entry_id, sex_type_id))
            self.conn.commit()
            print("Sex type for entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding sex type: {e}")
            return False
        return True

    # Add a position to an entry
    def add_entry_position(self, entry_id, position_id):
        """Add a position to an entry in the 'entry_position' table."""
        query = "INSERT INTO entry_position (entry_id, position_id) VALUES (?, ?)"
        try:
            self.cursor.execute(query, (entry_id, position_id))
            self.conn.commit()
            print("Position for entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding position: {e}")
            return False
        return True

    # Add a place to an entry
    def add_entry_place(self, entry_id, place_id):
        """Add a place to an entry in the 'entry_place' table."""
        query = "INSERT INTO entry_place (entry_id, place_id) VALUES (?, ?)"
        try:
            self.cursor.execute(query, (entry_id, place_id))
            self.conn.commit()
            print("Place for entry added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding place: {e}")
            return False
        return True

class Entry:
    def __init__(self, db, entry_id):
        """Initialize an Entry by fetching its details from the database."""
        self.db = db
        self.entry_id = entry_id

        # Fetch the entry details from the database
        entry_details = db.fetch_entry_with_details(entry_id)

        if entry_details:
            # Populate Entry attributes from fetched details
            entry_data = entry_details['entry']
            self.user_id = entry_data['user_id']
            self.date = self.parse_date(entry_data['date'])
            self.duration = entry_data['duration']
            self.note = entry_data['note']
            self.rating = entry_data['rating']
            self.initiator = entry_data['initiator']
            self.protection_used = entry_data['protection_used'] == "Yes"
            self.orgasms = entry_data['orgasms']
            self.partner_orgasms = entry_data['partner_orgasms']
            self.places = entry_details['places']
            self.sex_types = entry_details['sex_types']
            self.positions = entry_details['positions']
            self.position_ids = entry_details['position_ids']
        else:
            raise ValueError(f"Entry with ID {entry_id} not found in the database.")

    def parse_date(self, date_str):
        """Convert a date string into a Python date object."""
        return datetime.strptime(date_str, '%Y.%m.%d').date()

    def __repr__(self):
        """Return a nicely formatted string with all Entry attributes."""
        return (f"Entry ID: {self.entry_id}\n"
                f"User ID: {self.user_id}\n"
                f"Date: {self.date}\n"
                f"Duration: {self.duration} minutes\n"
                f"Note: {self.note}\n"
                f"Rating: {self.rating}/5 stars\n"
                f"Initiator: {self.initiator}\n"
                f"Protection Used: {self.protection_used}\n"
                f"Orgasms: {self.orgasms}\n"
                f"Partner's Orgasms: {self.partner_orgasms}\n"
                f"Places: {', '.join(self.places)}\n"
                f"Sex Types: {', '.join(self.sex_types)}\n"
                f"Positions: {', '.join(map(str, self.positions))}\n")
