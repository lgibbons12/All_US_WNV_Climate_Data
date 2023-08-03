months_dict = {
    "1": 31,
    "2": 28, #its 29 in 2000, 2004, 2008, 2012, 2016, and 2020
    "3": 31,
    "4": 30,
    "5": 31,
    "6": 30,
    '7': 31,
    "8": 31,
    '9': 30,
    "10": 31,
    "11": 30,
    "12": 31
}
leap_years = [2000, 2004, 2008, 2012, 2016, 2020]

def days_check(month: int, year):
    
    if month < 0 or month > 13:
        raise ValueError("Incorrect month")

    
    days = [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28'
    ]
    
    leap_year = year in leap_years

    if month == 2:
        if leap_year == False:
            return days
        else:
            days.append("29")
            return days
    
    days.append("30")
    
    num = months_dict[str(month)]

    if num == 30:
        return days

    days.append("31")
    return days


def get_month(month: int):
    if month < 0 or month > 13:
        raise ValueError("Incorrect month")
    if month < 10:
        return f"0{month}"
    return str(month)

