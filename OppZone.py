import click
from pathlib import Path
import postgis_load

all_colors = 'black', 'red', 'green', 'yellow', 'blue', 'magenta', \
             'cyan', 'white', 'bright_black', 'bright_red', \
             'bright_green', 'bright_yellow', 'bright_blue', \
'bright_magenta', 'bright_cyan', 'bright_white'


@click.group()
def cli():
    """Opp Zone Research Database Load:
    This CLI will guide you through the setup of code to run the Oppurtunity Zone Research data

    Running the repository requires that you, the user, go out and acquire select US Government data files
    and stage them for the analysis. In addition, you should have docker installed and have started up the 
    docker files with docker compose
    """

@cli.command('checkFiles')
def check_files():
    """Verify that the files are staged
    """
    click.echo(click.style("Verifying Census Tract Shape data for all states", fg='blue'))
    check_shape_files()
    check_acs_state_files()
    check_acs_tract_files()
    click.echo(click.style("All Files are Ready", fg='blue'))

@cli.command('loadDB')
def load_db():
    """Loads the Docker Datbase
    """
    click.echo(click.style("Loading the Datbase this takes 10 Min", fg='blue'))
    postgis_load.load_all()
    click.echo(click.style("LOADED", fg='green'))

def check_shape_files():
    shape_path =  Path('data', 'census_tract_shapes')
    if not Path('data').exists():
        click.echo(click.style("Creating the `data` directories", fg='red'))
        shape_path.mkdir(parents=True)
        click.echo(click.style("Run `OppZone download shapes` to download the files", fg='blue'))
    elif not shape_path.exists():
        click.echo(click.style("Creating the `data` directories", fg='red'))
        shape_path.mkdir(parents=True)
        click.echo(click.style("Run `OppZone download shapes` to download the files", fg='blue'))
    total_files = [f for f in shape_path.glob("*.json")]
    if len(total_files) == 56:
        click.echo(click.style(f"All {len(total_files)} tract files ready", fg='cyan'))
        return True
    else:
        click.echo(click.style("Missing at some Shape files Run `OppZone download shapes` to download the files", fg='red'))
        raise FileNotFoundError("Shape files not found")

def check_acs_state_files():
    state_file_path = Path('data','states','ACS_17_5yr_DP03_with_ann.csv')
    if not state_file_path.exists():
        error_message = """
        Please visit https://factfinder.census.gov/
        to download the DP03 Economic Characteristics file for all US States."""
        click.echo(click.style(error_message, fg='red'))
        raise FileNotFoundError("State Census files not found")
    else:
        return True

def check_acs_tract_files():
    state_file_path = Path('data','states','ACS_17_5yr_DP03_with_ann.csv')
    if not state_file_path.exists():
        error_message = """
        Please visit https://factfinder.census.gov/
        to download the DP03 Economic Characteristics file for all US Census Tracts."""
        click.echo(click.style(error_message, fg='red'))
        raise FileNotFoundError("Tract Economic files not found")
    else:
        return True