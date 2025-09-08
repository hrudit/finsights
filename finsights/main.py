import asyncio
from datetime import timedelta, datetime, time
from time import sleep
from finsights.services.downloader.pdf_downloader import download_pdfs
from finsights.services.fetcher.link_fetcher import create_transcript_list, transcripts_to_dbstate
from finsights.services.converter.pdf_to_text import convert_pdfs
from finsights.db.connection import set_tool_metadata, get_tool_metadata
from finsights.services.cleaner.file_cleaner import clean_up_files

if __name__ == "__main__":
    # Clean up the PDFs and text files from a week before todays date
    clean_up_files()
        
    # Get the date of the last run
    last_run_date = get_tool_metadata("last_run_date")
    if last_run_date:
        print(f"Hello! the last time you ran this tool was on {last_run_date}")
    # add code to check wif the time is between 9:15 am and 3:30 pm.
    # suggest to run the tool at a different time with a yes or no
    # proceed if they say yes
    if datetime.now().time() > time(9, 15) and datetime.now().time() < time(15, 30):
        print("BSE servers are very slow between 9:15 am and 3:30 pm.\nDo you want to proceed? (y/n): ")
        proceed = input("Do you want to proceed? (y/n): ")
        if proceed == "n":
            print("Please run the tool at a different time.")
            exit()


    print("Let's get you some transcript insights!")
    sleep(0.5)
    print("enter the date and well process insights for upto 5 days before that date")
    
    while True:
        try:
            date_input = input("Enter the date (DD-MM-YYYY): ")
            date_input = datetime.strptime(date_input, "%d-%m-%Y").date()
            break
        except ValueError:
            print("Invalid date format. Please enter the date in DD-MM-YYYY format (e.g., 25-01-2025)")
    print(f"We will process insights for upto 5 days before {date_input.strftime('%d-%m-%Y')}")

    start_date = date_input - timedelta(days=1)
    transcript_list = asyncio.run(create_transcript_list(start_date, date_input))
    transcript_uuids = transcripts_to_dbstate(transcript_list)
    asyncio.run(download_pdfs(transcript_uuids))
    convert_pdfs()

    if last_run_date != datetime.now().strftime("%d-%m-%Y"):
        # Set the date of the last run
        set_tool_metadata("last_run_date", datetime.now().strftime("%d-%m-%Y"))
    