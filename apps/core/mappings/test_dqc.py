test_dqc_json = {
    "basics": {
        "shape": "schemas/trans_schemas.py",
        "values": {
            "ID": "isNaturalInteger",
            "UserName": {
                
            },
            "RealName": {

            },
            "Contact": {

            },
            "Orders": {

            },
            "Items": {

            },
            "Quantity": {

            },
            "CreatedDate": {

            },
        },
        "keys": {
            "unique": ["ID"],
            "composite": [(),()],
        }
    },

    "groupings": {
        "group_1": {
            "Name": "Contact",
            "Function": "contact_quantity_dqc",
            "Parameters": ["Test", "Quantity", 10],
            "Expected": "records with Contact == 'Test' have no more than 10 units of Quantity",
        }
    }
}