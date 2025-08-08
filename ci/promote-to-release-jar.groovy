import org.scalactic.Many

def TARGET_DEPLOYMENT_PATH
def TARGET_SERVER_NAME
def TARGET_SERVICE_ACCOUNT

// The following variables were declared and sourced from jenkins-bootstrap-nonprod repo jobs/build_branch_man_params_deployment.groovy
// def DEPLOYMENT_ENVIRONMENT = ''
// def CHANGE_REQUEST = ''

pipeline {

    options {
        disableConcurrentBuilds()
        timestamp()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    agent none // specified on a stage basis

    // environment variables available for all stages
    environment {
        RELEASE_SYMLINK_NAME = "kyc-sc-release-spark.jar"
        CANDIDATE_SYMLINK_NAME = "kyc-sc-spark.jar"

        NO_PROXY_HOST = "artifactory.ext.national.com.au"
        NAB_PROXY_HOST = "forwardproxy"
        NAB_PROXY_PORT = "3128"

        HTTP_PROXY = "http://forwardproxy:3128"
        HTTPS_PROXY = "https://forwardproxy:3128"
        NO_PROXY = "localhost, 169.254.169.254, .thenational.com, .national.com.au, idp.intapi.ext.national.com.au, cloudplatfm.intapi.ext"
        PROXY_ENABLED = true
    }

    stages {
        stage('Setup') {
            steps {
                script {
                    // ensure DEPLOYMENT_ENVIRONMENT has the correct value
                    if(!['prod', 'non-prod'].contains(DEPLOYMENT_ENVIRONMENT)){
                        currentBuild.result = 'FAILURE'
                        error 'Deployment environment is not valid. Acceptable values are "prod" and "non-prod".'
                    }

                    // ensure user has a CR before deploying to production
                    if(DEPLOYMENT_ENVIRONMENT == 'prod' && CHANGE_REQUEST.isEmpty()) {
                        currentBuild.result = 'FAILURE'
                        error 'Requirements not met. If deploying to prod, you need to have a valid Change Request'
                    }

                    echo "DEPLOYMENT_ENVIRONMENT: ${DEPLOYMENT_ENVIRONMENT}"
                    echo "CHANGE_REQUEST: ${CHANGE_REQUEST}"

                    // get environment variables from file
                    environmentConfig = readYaml (file: "ci/lib/environments.yaml")

                    TARGET_DEPLOYMENT_PATH = environmentConfig[DEPLOYMENT_ENVIRONMENT]["TARGET_DEPLOYMENT_PATH"]
                    TARGET_SERVER_NAME = environmentConfig[DEPLOYMENT_ENVIRONMENT]["TARGET_SERVER_NAME"]
                    TARGET_SERVICE_ACCOUNT = environmentConfig[DEPLOYMENT_ENVIRONMENT]["TARGET_SERVICE_ACCOUNT"]

                    // Check for non-null variables
                    if(
                        TARGET_DEPLOYMENT_PATH.isEmpty() ||
                        TARGET_SERVER_NAME.isEmpty() ||
                        TARGET_SERVICE_ACCOUNT.isEmpty() ||
                    ){
                        currentBuild.result = 'FAILURE'
                        error 'ERROR: One or more targe environemnt inputs are empty'
                    }

                    env.SSH_TARGET = "${TARGET_SERVICE_ACCOUNT}@${TARGET_SERVER_NAME}"

                    echo "TARGET_DEPLOYMENT_PATH: ${TARGET_DEPLOYMENT_PATH}"
                    echo "RELEASE_SYMLINK_NAME: ${RELEASE_SYMLINK_NAME}"

                    // Retrieve key to connect to onprem server via ssh/scp
                    echo "Retrieving the key top connect to the server"
                    sh "mkdir -p ssh-onprem"
                    sh "ls -l ssh-onprem/"
                    sh """
                    mkdir -p ~/.aws/
                    echo -e "[profile default]\n region=ap-southeast-2" > ~/.aws/config
                    aws ssm get-parameter --with-decryption --name /jenkins/sas_onprem/sshkeys/private --query Parameter.Value --output text > ssh-onprem/onprem_id_rsa && chmod 400 ssh-onprem/onprem_id_rsa
                    aws ssm get-parameter --with-decryption --name /jenkins/sas_onprem/sshkeys/public -- query Parameter.Value --output text > ssh-onprem/onprem_id_rsa.pub && chmod 600 ssh-onprem/onprem_id_rsa.pub                   
                    """
                }
            }
        }

        stage('Fetch artefact and promote') {
            agent any
            steps {
                script {
                    // Promote Jar to release
                    promoteToRelease(env.SSH_TARGET, TARGET_DEPLOYMENT_PATH, RELEASE_SYMLINK_NAME, CANDIDATE_SYMLINK_NAME)
                }
            }
        }
    }

}

def promoteToRelease(targetHost, remoteFolder, releaseSymLinkName, candidateSymLinkName) {
    echo "PROMOTE JAR FILE STARTED -----------------------------------"
    echo "Promoting jar file in host ${targetHost} - candidateSymLinkName ${candidateSymLinkName}"
    echo "Finding real path of ${candidateSymLinkName}"

    realPathCommand = """
    ssh -i ssh-onprem/onprem_id_rsa -o StrictHostKeyChecking=no -o UserKnownHostFile=/dev/null \
    ${targetHost} 'readlink -f ${remoteFolder}/${candidateSymLinkName}'
    """
    realPathOuput = sh(script: realPathCommand, returnStdout: true)

    if(realPathOuput.isEmpty() || !realPathOuput.contains("-assembly.jar")) {
        currentBuild.result = 'ERROR'
        error("Could not find real path of symbolic link ${candidateSymLinkName}")
    }

    echo "Real path of ${candidateSymLinkName} = ${realPathOuput}"
    echo "Creating symbolic link ${releaseSymLinkName} using real path of ${candidateSymLinkName}"
    // -s=symbolic and -f=Overwrite (force) the destination path of the symlink
    ssh """
    ssh -i ssh-onprem/onprem_id_rsa -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    ${targetHost} 'ln -sf ${realPathOuput.trim()} ${remoteFolder}/${releaseSymLinkName} && ls -la ${remoteFolder}'
    """

    echo "PROMOTE JAR FILE COMPLETED -----------------------------------"
}