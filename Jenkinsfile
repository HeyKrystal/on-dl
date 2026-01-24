// For helper calls.
@Library('jenkins-pipeline-helper-shared-library@main') _

import groovy.json.JsonSlurperClassic
import groovy.json.JsonOutput

pipeline {
  agent { label 'oakbeaver' } // adjust if your agents use a different label

  options {
    timestamps()
    ansiColor('xterm')
    disableConcurrentBuilds() // avoids overlapping main deploys
  }

  environment {
    // Repo name used for deploy path: ${deployRoot}/${REPO_NAME}
    //REPO_NAME = helper.repoName()

    // Container used for validate steps
    PY_IMAGE = 'python:3.12-slim'

    // Keep the latest N releases on FrostedStoat
    KEEP_RELEASES = '5'

    // Target selector (must exist as a top-level key in your JSON)
    DEPLOY_TARGET = 'frostedstoat'

    // Jenkins Credential IDs (create these in Jenkins)
    DEPLOY_TARGETS_JSON_CRED = 'deploy-target-frostedstoat' // Secret Text JSON (your map of targets)
    FROSTED_SSH_CRED         = 'ssh-frostedstoat-target'           // SSH Username with private key
    DISCORD_WEBHOOK_CRED     = 'krystal-net-webhook-url'       // Secret Text (Discord webhook URL)
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
        sh 'git rev-parse HEAD > GIT_COMMIT.txt'
      }
    }

    stage('Validate (lint + tests)') {
      steps {
        sh '''
          set -eux

          # Run validation inside a fresh container on the agent
          docker run --rm -t \
            -u "$(id -u):$(id -g)" \
            -e HOME=/tmp \
            -e PIP_CACHE_DIR=/tmp/pip-cache \
            -e PATH="/tmp/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
            -v "$PWD:/work" -w /work \
            "$PY_IMAGE" \
            sh -lc '
              set -eux

              # If an old root-owned cache exists, avoid using it at all.
              # Deleting may fail if it is root-owned; ignore that.
              rm -rf .ruff_cache || true

              python --version
              python -m pip install --upgrade pip

              # Install project + dev deps (if present)
              if [ -f requirements.txt ]; then
                python -m pip install -r requirements.txt
              fi

              if [ -f requirements-dev.txt ]; then
                python -m pip install -r requirements-dev.txt
              fi

              # Lint (ruff) - fail build if lint fails
              python -m ruff --version >/dev/null 2>&1 && python -m ruff check . --no-cache

              # Tests (pytest) - run only if tests exist
              if find . -type f \\( -name "test_*.py" -o -name "*_test.py" \\) -print -quit 2>/dev/null | grep -q .; then
                python -m pytest -q --disable-warnings --maxfail=1
              else
                echo "No tests found; skipping pytest."
              fi

              # Extra sanity: ensure files compile
              python -m compileall -q .
            '
        '''
      }
    }

    stage('Package artifact') {
      steps {
        script {
          def sha = sh(script: "cat GIT_COMMIT.txt | cut -c1-12", returnStdout: true).trim()
          env.SHORT_SHA = sha
          env.ARTIFACT_FILE = "${env.RELEASE_TAG}.tar.gz"
          env.ARTIFACT_PATH = "distro/${env.ARTIFACT_FILE}"
          env.RELEASE_TAG = "ondl-${env.BRANCH_NAME}-${env.BUILD_NUMBER}-${sha}"
        }

        sh '''
          # Create a deployable tarball from the checked-out workspace
          set -eux
          mkdir -p distro
          git ls-files -z | tar --null -T - -czf "$ARTIFACT_PATH"
        '''
        archiveArtifacts artifacts: "${ARTIFACT_PATH},GIT_COMMIT.txt", fingerprint: true
      }
    }

    stage('Deploy to FrostedStoat (main only)') {
      when { branch 'main' }
      steps {
        script { env.DID_DEPLOY = "false" }

        withCredentials([
          string(credentialsId: "${DEPLOY_TARGETS_JSON_CRED}", variable: 'DEPLOY_TARGETS_JSON'),
          sshUserPrivateKey(credentialsId: "${FROSTED_SSH_CRED}", keyFileVariable: 'SSH_KEY', usernameVariable: 'SSH_USER')
        ]) {
          script {
            def targets = new JsonSlurperClassic().parseText(env.DEPLOY_TARGETS_JSON)
            def t = targets[env.DEPLOY_TARGET]
            if (t == null) {
              error("Unknown DEPLOY_TARGET='${env.DEPLOY_TARGET}'. Available: ${targets.keySet()}")
            }
            // Your JSON uses: host, user, deployRoot
            env.T_HOST = "${t.host}"
            env.T_USER = "${t.user}"
            env.T_DEPLOY_ROOT = "${t.deployRoot}"

            // Derived paths
            env.T_APP_DIR = "${env.T_DEPLOY_ROOT}/${helper.repoName()}"
            env.T_RELEASES_DIR = "${env.T_APP_DIR}/releases"
            env.T_RELEASE_DIR = "${env.T_RELEASES_DIR}/${env.RELEASE_TAG}"
            env.T_CURRENT_LINK = "${env.T_APP_DIR}/current"
          }

          sh '''
            set -eux

            # Make sure target dirs exist
            ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$T_USER@$T_HOST" "
              set -eux
              mkdir -p '$T_RELEASES_DIR'
            "

            # Copy the artifact to target temp
            scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$ARTIFACT_PATH" \
              "$T_USER@$T_HOST:/tmp/$ARTIFACT_PATH"

            # Extract to new release dir, flip current symlink, keep last N releases
            ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$T_USER@$T_HOST" "
              set -eux

              # Optional runtime preflight (doesn't reinstall anything)
              command -v ffmpeg >/dev/null 2>&1 || echo 'WARN: ffmpeg not found in PATH on FrostedStoat'
              command -v yt-dlp >/dev/null 2>&1 || echo 'WARN: yt-dlp not found in PATH on FrostedStoat'
              command -v curl  >/dev/null 2>&1 || echo 'WARN: curl not found in PATH on FrostedStoat'

              mkdir -p '$T_RELEASE_DIR'
              tar -xzf '/tmp/$ARTIFACT_PATH' -C '$T_RELEASE_DIR'
              rm -f '/tmp/$ARTIFACT_PATH'

              # Atomic-ish cutover: update the symlink in one operation
              ln -sfn '$T_RELEASE_DIR' '$T_CURRENT_LINK'

              # Cleanup: keep the newest $KEEP_RELEASES release directories
              # (based on modification time)
              cd '$T_RELEASES_DIR'
              if [ -d . ]; then
                ls -1dt ./* 2>/dev/null | tail -n +$((KEEP_RELEASES+1)) | xargs -I{} rm -rf \"{}\"
              fi
            "
          '''

          script { env.DID_DEPLOY = "true" }
        }
      }
    }
  }

  post {
    always {
      script {
        // timing
        long startMs = currentBuild.startTimeInMillis ?: System.currentTimeMillis()
        long endMs = System.currentTimeMillis()
        long durSec = ((endMs - startMs) / 1000L)

        def result = currentBuild.currentResult ?: "UNKNOWN"
        def sha = env.SHORT_SHA ?: sh(script: "cat GIT_COMMIT.txt | cut -c1-12", returnStdout: true).trim()
        def deployed = env.DID_DEPLOY ?: "false"

        // Severity & colors (your palette)
        // SUCCESS = 0x2ECC71, INFO = 0x3498DB, WARNING = 0xF1C40F, CHANGED = 0x9B59B6, FAILED/ERROR = 0xE74C3C
        int color
        String author
        if (result == "SUCCESS") {
          color = 0x2ECC71
          author = "✅ BUILD COMPLETE"
        } else if (result == "UNSTABLE") {
          color = 0xF1C40F
          author = "⚠️ BUILD UNSTABLE"
        } else {
          color = 0xE74C3C
          author = "❌ BUILD FAILED"
        }

        String summary =
          "Branch: ${env.BRANCH_NAME}\\n" +
          "Commit: ${sha}\\n" +
          "Deployed: ${deployed}" + (env.BRANCH_NAME == 'main' ? " (keep last ${env.KEEP_RELEASES})" : "") + "\\n" +
          "Duration: ${durSec}s\\n" +
          "Build: ${env.BUILD_URL}"

        def embed = [
          author: [ name: author ],
          description: summary,
          color: color,
          footer: [ text: "IronKerberos • Jenkins" ],
          fields: [
            [ name: "Job", value: "${env.JOB_NAME}", inline: false ],
            [ name: "Agent", value: "${env.NODE_NAME ?: 'n/a'}", inline: true ],
            [ name: "Build #", value: "${env.BUILD_NUMBER}", inline: true ]
          ]
        ]

        def payload = JsonOutput.toJson([
          username: "IronKerberos",
          embeds: [ embed ]
        ])

        withCredentials([string(credentialsId: "${DISCORD_WEBHOOK_CRED}", variable: 'DISCORD_WEBHOOK_URL')]) {
          // Use curl from the agent host (not inside a container)
          sh """
            set -eux
            curl -sS -H 'Content-Type: application/json' -d '${payload.replace("'", "'\\''")}' "\$DISCORD_WEBHOOK_URL" >/dev/null
          """
        }
      }
    }
  }
}
