#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[2]
JAVA = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"


class TestBuildInfoGit(unittest.TestCase):
    """Execute the actual Kotlin metadata functions in isolated Git repositories."""

    def run_command(self, argv, cwd):
        result = subprocess.run(argv, cwd=cwd, text=True, capture_output=True,
                                env=dict(os.environ, JAVA_HOME=JAVA), timeout=180)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        return result.stdout.strip()

    def test_generated_metadata_in_main_linked_detached_and_packed_refs(self):
        with tempfile.TemporaryDirectory(prefix="gravitino-build-info-") as directory:
            root = Path(directory) / "main"
            root.mkdir()
            self.run_command(["git", "init", "-b", "main"], root)
            (root / "settings.gradle.kts").write_text('rootProject.name = "metadata-probe"\n')
            script = "import java.io.ByteArrayOutputStream\nimport java.text.SimpleDateFormat\nimport java.util.Date\nplugins { java }\n"
            for module in ("common", "server"):
                text = (ROOT / module / "build.gradle.kts").read_text()
                function = text[text.index("fun getGitCommitId()"):]
                function = function[:function.index("\n}\n") + 3]
                script += function.replace("getGitCommitId", module + "GitCommitId") + "\n"
            text = (ROOT / "common/build.gradle.kts").read_text()
            writer = text[text.index('val propertiesFile ='):text.index('\nval generateBuildInfo') if '\nval generateBuildInfo' in text else text.index('\ntasks {')]
            script += writer.replace("getGitCommitId", "commonGitCommitId")
            script += """
version = "probe"
tasks.register("metadataProbe") {
  doLast {
    writeProjectPropertiesFile()
    file("server-build-info.properties").writeText("git.commit.id=${serverGitCommitId()}\\n")
  }
}
"""
            script += text[text.index("\nval generateBuildInfo") if "\nval generateBuildInfo" in text else text.index("\ntasks {"):].replace("getGitCommitId", "commonGitCommitId")
            (root / "build.gradle.kts").write_text(script)
            self.run_command(["git", "add", "."], root)
            self.run_command(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                              "commit", "-m", "metadata fixture"], root)
            linked = Path(directory) / "linked"
            detached = Path(directory) / "detached"
            self.run_command(["git", "worktree", "add", "-b", "linked", str(linked)], root)
            self.run_command(["git", "worktree", "add", "--detach", str(detached)], root)
            # Pack branch refs before all three probes: neither loose refs nor .git directories
            # are required by Git, and generated metadata must agree with Git itself.
            self.run_command(["git", "pack-refs", "--all"], root)
            archive = Path(directory) / "source-archive"
            archive.mkdir()
            for name in ("settings.gradle.kts", "build.gradle.kts"):
                (archive / name).write_bytes((root / name).read_bytes())
            for checkout in (root, linked, detached, archive):
                with self.subTest(checkout=checkout.name):
                    expected = "" if checkout == archive else self.run_command(["git", "rev-parse", "HEAD"], checkout)
                    self.run_command([str(ROOT / "gradlew"), "-p", str(checkout), "metadataProbe",
                                      "--offline", "--no-daemon", "--max-workers=1",
                                      "-Dorg.gradle.jvmargs=-Xmx512m", "--console=plain"], checkout)
                    for artifact in ("src/main/resources/gravitino-build-info.properties",
                                     "server-build-info.properties"):
                        properties = dict(line.split("=", 1) for line in
                                          (checkout / artifact).read_text().splitlines()
                                          if "=" in line and not line.startswith("#"))
                        self.assertEqual(expected, properties["git.commit.id"], artifact)

            # Exercise the actual task wiring twice without clean: jar.doFirst is too late
            # to regenerate processResources output and duplicate exclusion keeps stale bytes.
            command=[str(ROOT / "gradlew"), "-p", str(root), "jar", "--offline", "--no-daemon",
                     "--max-workers=1", "-Dorg.gradle.jvmargs=-Xmx512m", "--console=plain"]
            self.run_command(command,root)
            self.run_command(["git","-c","user.name=Fixture","-c","user.email=fixture@example.invalid",
                              "commit","--allow-empty","-m","second metadata revision"],root)
            expected=self.run_command(["git","rev-parse","HEAD"],root)
            self.run_command(command,root)
            with zipfile.ZipFile(root/"build/libs/metadata-probe-probe.jar") as archive:
                self.assertIn("git.commit.id="+expected,archive.read("gravitino-build-info.properties").decode())
            self.assertIn("git.commit.id="+expected,(root/"build/resources/main/gravitino-build-info.properties").read_text())


if __name__ == "__main__":
    unittest.main()
