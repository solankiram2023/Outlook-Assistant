# Function to get initials from name
def get_initials(name):
    return ''.join(word[0].upper() for word in name.split() if word)

# Function to get category color
def get_category(email):
    categories = {
        'Work'              : ("Work", "#e8f0fe"),
        'Client Issue'      : ("Client Issue", "#fce8e6"),
        'Marketing'         : ("Marketing", "#e6f4ea"),
        'IT Support'        : ("IT Support", "#fff0e0"),
        'Legal/Contracts'   : ("Legal/Contracts", "#f3e8fd"),
        'HR/Benefits'       : ("HR/Benefits", "#f7f8fb")
    }
    
    category_name, color = categories.get(email['category'], ("Other", "#f0f0f0"))
    return category_name, color