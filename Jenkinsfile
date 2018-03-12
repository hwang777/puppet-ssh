pipeline
{
	agent any
	
	environment {
        PROJ = 'Neo4J SP'
        MULTIBRANCH    = 'TRUE'
    }
	
	options
	{
		buildDiscarder(logRotator(numToKeepStr: '5', artifactNumToKeepStr: '2'))
	}
	stages
	{

		stage("build")
		{
			steps
			{
				sh 'mvn test'
			}
		}
		stage("test api")
		{
			steps
			{
				sh 'mvn test'
			}
		}

	}
	post
	{

		always
		{
			junit "target/surefire-reports/*.xml"
			echo "$PROJ Build $BUILD_DISPLAY_NAME - $BUILD_ID - $BUILD_NUMBER - $BUILD_TAG"
			emailext(
				subject: "$PROJ Build $BUILD_DISPLAY_NAME",
				body:  "$BUILD_ID - $BUILD_NUMBER - $BUILD_TAG",
				to: "henry.wang@bestit.com"
			)
		}
	}

}

