from utils import load_json_config, time_function
from helper import RetiredIDProcessor
from crosswalk_to_SDU import crosswalk_to_SDU
from match_nomatch_procedure import generate_match_nomatch


CONFIG_LOCATION = "C:\\Git\\LDOE\\ACT\\23\\match_nomatch\\config_match_no_match.json"

@time_function
def main():
    # Load the configuration
    config = load_json_config(CONFIG_LOCATION)
    # Create the ID processor
    id_processor = RetiredIDProcessor(config['retire_id_filepath'])
    # Generate the match/no match files, and data for crosswalk to SDU
    nov_lap_11, student_roster_4 = generate_match_nomatch(config, id_processor)
    # Generate SDU files
    crosswalk_to_SDU(config, nov_lap_11, student_roster_4)


if __name__ == '__main__':
    main()
