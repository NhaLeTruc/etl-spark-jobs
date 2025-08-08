application_environments {
    app {
        environment {
            APP_NAME='kyc-sc-spark' // update to your project name

            // ENVIRONMENT
            BDA_FOLDER='kyc-sc' // for on prem deployment, folder to push jar files into

            // List of users allowed to approve promotion and deployments to prod
            // XXXX pXXXX
            // XXXX pXXXX
            // XXXX pXXXX
            // XXXX pXXXX
            // XXXX pXXXX
            // XXXX pXXXX
            // XXXX pXXXX

            def APPROVERS='pXXXX, pXXXX, pXXXX, pXXXX, pXXXX, pXXXX'

            // also defined in sbt file
            ARTIFACTORY_ORG_PATH = 'au/com/nab/kyc-sc' // change 'odyssey' with your team name

            // ***************************
            // REPO TO PUSH BUILT ARTEFACTS TO
            // -build will be replaced by -verify or -release at later stage of pipeline
            // ***************************
            ARTIFACTORY_REPO = 'FCMP-SBT-build' // prod

            // ***************************
            // Build agent (docker based)
            // If DOCKER_REPO points to ECR, it will be detected and login to docker will work
            // ***************************

            BASE_DOCKER_BUILD_IMAGE_NAME = 'scala-sbt-agent'
            BASE_DOCKER_BUILD_IMAGE_LABEL = 'scala-2.11.8-sbt-1.3.10-0.1.0-16-742c281'
            // DOCKER_REPO = "fcmp-docker-build.artifactory.ext.national.com.au"
            DOCKER_REPO = "548238242060.dkr.ecr.sap-southeast-2.amazonaws.com"

        }
    }

    ppte {
        enabled = true
        update_spark_job_config_enabled =false
        target = 'srv-lasrbda-st@dpbdt01acn01.oes.esptest.aurtest.national.com.au' // host to deploy to using SSH/SCP
        artefactory_repository_name_suffix = '-verify' // which repository to fetch the artefact from. Could be: build or verify or release
    }
}

libraries {
    // deputy is included here (instead of config tier) so we can specify the app IDs
    deputy {
        enabled = true
        cli_version = "0.11.0"
        deputy_app_id = "dfde6a63=8369-4de1-a719-6b4e3aa9db0c"
        deputy_credentails_id = "KYC_SNAPSHOT_DEPUTY_DAF_CLIENT_DEVOPS"
        deputy_environment = 'prod' // prod. Defaults to prod if unspecified
    }

    notification {
        all_branches_enabled = false
        office365_common_credential_id = 'KYC_SNAPSHOT_COMMON_OFFICE365_HOOK'
        office365_failure_credential_id = 'KYC_SNAPSHOT_FAILURE_OFFICE365_HOOK'
    }

    tsr {
        enabled = false
        always_run = true
    }
}

stages {
    stage_promote_to_release {
        user_confirmation
    }

    stage_deploy_to_prod {
        user_confirmation
    }

    stage_confirm_deploy_to_prod_success {
        user_confirmation
    }
}