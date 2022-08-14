CHARITY_MODE_MOSTRECENT = "mostrecent"
CHARITY_QUERY_MOSTRECENT = [
                {
                    '$addFields': {
                        'mostRecentYr': {
                            '$last': '$years'
                        }
                    }
                }, {
                    '$unset': 'years'
                }
            ]
CHARITY_MODE_RECENT = "recent"
CHARITY_QUERY_RECENT = [
                {
                    '$addFields': {
                        'recentYears': {
                            '$slice': [
                                '$years', -3
                            ]
                        }
                    }
                }, {
                    '$unset': 'years'
                }, {
                    '$unwind': {
                        'path': '$recentYears',
                        'includeArrayIndex': 'recentIndex',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$replaceRoot': {
                        'newRoot': {
                            '$mergeObjects': [
                                '$$ROOT', '$recentYears'
                            ]
                        }
                    }
                }, {
                    '$unset': 'recentYears'
                }
            ]
CHARITY_MODE_OTHER = "other"
CHARITY_QUERY_OTHER = [{
                '$match': {
                    'years.year': None
                }
            }, {
                '$project': {
                    'countryCode': 1,
                    'charityNumber': 1,
                    'businessNumber': 1,
                    'targetYr': {
                        '$filter': {
                            'input': '$years',
                            'as': 'year',
                            'cond': {
                                '$eq': [
                                    '$$year.year', None
                                ]
                            }
                        }
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': [
                            '$$ROOT', {
                                '$first': '$targetYr'
                            }
                        ]
                    }
                }
            }, {
                '$unset': 'targetYr'
            }
            ]

CHARITY_MODE = CHARITY_MODE_OTHER
CHARITY_QUERY = CHARITY_QUERY_OTHER
