# /bin/sh

# parameters: 0-application, 1-applicatinType(in-edge model or graphchi model)

if [ $# -ne 2 ]
then 
	echo "Please check the number of parameters!!!"
	exit 2
fi

appName=$1
appType=$2

if [ ${appType} = "in-edge" ]
then
	`make ${appName}`
elif [ ${appType} = "graphchi" ]
then
	cp "./application/"${appName}".cpp" "./graphchi/example_apps/"${appName}".cpp"
	appFilePath="example_apps/"${appName}
	make -C ./graphchi ${appFilePath}
	mv "./graphchi/bin/example_apps/"${appName} "bin/"${appName}
	rm "./graphchi/"${appFilePath}".cpp"
else
	echo "The application type must be in-edge or graphchi"
fi
	
