from datetime import datetime, timedelta



def today():
    # Get the current date
    current_date = datetime.now()
    # Format it as 'YYYY-MM-DD'
    return current_date.strftime('%Y-%m-%d')


def first_day_of_current_month():
    # Get the current date
    current_date = datetime.now()
    # Get the first day of the current month and year
    first_day_of_month = current_date.replace(day=1)
    # Format it as 'DD-MM-YYYY'
    return first_day_of_month.strftime('%Y-%m-%d')


def first_day_of_previous_month():
    # Get the current date
    current_date = datetime.now()
    # Subtract one month from the current date
    first_day_current_month = current_date.replace(day=1)
    previous_month_date = first_day_current_month - timedelta(days=1)
    # Get the first day of the previous month
    first_day_previous_month = previous_month_date.replace(day=1)
    # Format it as 'DD-MM-YYYY'
    return first_day_previous_month.strftime('%Y-%m-%d')


def first_day_of_previous_year():
    current_date = datetime.now()
    first_day_previous_year = current_date.replace(
        year=current_date.year - 1,  # Subtract 1 year
        month=1,                     # Set to January
        day=1                        # Set to 1st day
    )
    return first_day_previous_year.strftime('%Y-%m-%d')  # Format as 'YYYY-MM-DD'

