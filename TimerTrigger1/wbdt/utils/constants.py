databases = {
        'account': ['accounts', 'accountusers', 'userprofiles'],
        'action': ['actions', 'actioninstances', 'actionresults'],
        'listing': ['listings', 'claims', 'connections'],
        'charity': ['charities'],
        'engage': ['engageprofiles',
                   'causesubscriptions', 'ministrylistingchannelsubscriptions',
                   "reflectionresults"],
        'community': ['communities'],
        'conversation': ['conversations', 'messages'],
        'content': ['channels', 'contentitems'],
        'notification': ['notifications', 'triggers'],
        'web-search': ['filters', 'notifications', 'savedsearches', 'subscriptions'],
        'program': ['programs'],
        'cause': ['causes', 'collaborators'],
        # "video-subscription": ["subscriptions"]   # Not filters in Dev MongoDB
    }
ELASTICSEARCH_LOCATION = "https://b4c19f26f8924b4f8e8f716aee638843.eastus2.azure.elastic-cloud.com:9243/"

NS_SPLIT_BLOCK_SIZE = 100


ZOHO_MODELS_LIST = [
                   "Accounts",
                   "Orgs",
                   "Contacts",
                   "Contacts_X_Accounts",
                   "Contacts_X_Orgs",
                   "ChargeBee_Data",
                   #"Potentials"
                   #"Invalid_Contacts"
                   ]
ZOHO_URL_TOKEN = "https://accounts.zoho.com/oauth/v2/token?"
ZOHO_URL_BULK = "https://www.zohoapis.com/crm/bulk/v2/read"
ZOHO_URL_SETTINGS = "https://www.zohoapis.com/crm/v2/settings/fields?module="
