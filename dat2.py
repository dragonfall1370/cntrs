from dat import Reporting_variables

variables = Reporting_variables()

yesterday = variables.yesterday()

print(yesterday)

for module_name in ["campaign", "loyalty", "reward", "test", "test2"]:
	if module_name in ["campaign", "loyalty", "reward"]:
		print(module_name)
