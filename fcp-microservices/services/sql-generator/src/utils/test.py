import yaml



with open('testing_config.yml') as stream:
    try:
        TEST_SETTINGS = yaml.safe_load(stream)
    except:
        print(exec)



