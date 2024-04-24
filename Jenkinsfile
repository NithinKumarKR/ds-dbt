@Library('jenkins-library') _
pipeline {
  agent {
    kubernetes(k8sAgent(resourceType: 'mini', appType: 'JAVA_17'))
  }
  stages {
    stage('Docker Build') {
      steps {
        container('m2p-base-kaniko') {
          buildDocker('m2pfintech01/airbyte:dbt-project-' + env.GIT_COMMIT, "./dbt_project/Dockerfile")
        }
      }
    }
    stage('Docker Push') {
      steps {
        container('m2p-base-build') {
          pushDocker('m2pfintech01/airbyte:dbt-project-' + env.GIT_COMMIT)
        }
      }
    }
  }
}