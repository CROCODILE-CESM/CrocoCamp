"""Tests for mocking perfect_model_obs subprocess calls.

This module tests that perfect_model_obs subprocess calls can be properly
mocked, allowing the workflow to run without requiring DART executables.
Tests cover successful execution, failures, and output file generation.
"""

import os
import pytest
from pathlib import Path
from unittest.mock import patch, Mock, mock_open
import subprocess


class TestPerfectModelObsMocking:
    """Test mocking of perfect_model_obs subprocess calls."""
    
    def test_mock_successful_execution(self, tmp_path):
        """Test mocking successful perfect_model_obs execution."""
        # Create a mock process
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.wait.return_value = None
        
        with patch('subprocess.Popen', return_value=mock_process) as mock_popen:
            # Simulate calling perfect_model_obs
            logfile = tmp_path / "perfect_model_obs.log"
            with open(logfile, "a") as log:
                process = subprocess.Popen(
                    ["perfect_model_obs"],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                process.wait()
            
            # Verify the mock was called correctly
            mock_popen.assert_called_once()
            args, kwargs = mock_popen.call_args
            assert args[0] == ["perfect_model_obs"]
            assert kwargs['stdout'] is not None
            assert kwargs['stderr'] == subprocess.STDOUT
            assert process.returncode == 0
    
    def test_mock_execution_failure(self, tmp_path):
        """Test mocking perfect_model_obs execution failure."""
        # Create a mock process that fails
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.wait.return_value = None
        mock_process.stderr = Mock()
        mock_process.stderr.read.return_value = "DART ERROR: Invalid namelist"
        
        with patch('subprocess.Popen', return_value=mock_process):
            # Simulate calling perfect_model_obs
            logfile = tmp_path / "perfect_model_obs.log"
            with open(logfile, "a") as log:
                process = subprocess.Popen(
                    ["perfect_model_obs"],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                process.wait()
            
            # Verify failure is detected
            assert process.returncode == 1
    
    def test_mock_with_output_file_generation(self, tmp_path):
        """Test mocking perfect_model_obs with output file creation."""
        # Create a side effect that generates output files
        def create_output_files(*args, **kwargs):
            # Simulate perfect_model_obs creating obs_seq.final
            output_file = tmp_path / "obs_seq.final"
            output_file.write_text("mock observation sequence output")
            
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.wait.return_value = None
            return mock_process
        
        with patch('subprocess.Popen', side_effect=create_output_files):
            # Change to tmp directory
            original_dir = os.getcwd()
            os.chdir(tmp_path)
            
            try:
                # Simulate calling perfect_model_obs
                logfile = tmp_path / "perfect_model_obs.log"
                with open(logfile, "a") as log:
                    process = subprocess.Popen(
                        ["perfect_model_obs"],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True
                    )
                    process.wait()
                
                # Verify output file was created
                output_file = tmp_path / "obs_seq.final"
                assert output_file.exists()
                assert output_file.read_text() == "mock observation sequence output"
            finally:
                os.chdir(original_dir)
    
    def test_mock_with_log_output(self, tmp_path):
        """Test that perfect_model_obs output is captured in log file."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.wait.return_value = None
        
        with patch('subprocess.Popen', return_value=mock_process):
            # Create log file
            logfile = tmp_path / "perfect_model_obs.log"
            with open(logfile, "a") as log:
                log.write("perfect_model_obs starting...\n")
                log.write("processing observations...\n")
                log.write("perfect_model_obs finished successfully\n")
                
                process = subprocess.Popen(
                    ["perfect_model_obs"],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                process.wait()
            
            # Verify log file contains expected content
            log_content = logfile.read_text()
            assert "perfect_model_obs starting" in log_content
            assert "processing observations" in log_content
            assert "finished successfully" in log_content
    
    def test_mock_with_cwd_parameter(self, tmp_path):
        """Test that perfect_model_obs respects working directory."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.wait.return_value = None
        
        with patch('subprocess.Popen', return_value=mock_process) as mock_popen:
            # Simulate calling with specific working directory
            work_dir = str(tmp_path / "work")
            os.makedirs(work_dir, exist_ok=True)
            
            logfile = tmp_path / "perfect_model_obs.log"
            with open(logfile, "a") as log:
                process = subprocess.Popen(
                    ["perfect_model_obs"],
                    cwd=work_dir,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                process.wait()
            
            # Verify cwd was passed
            _, kwargs = mock_popen.call_args
            assert kwargs['cwd'] == work_dir


class TestPerfectModelObsPathResolution:
    """Test mocking perfect_model_obs path resolution."""
    
    def test_mock_executable_path(self, tmp_path):
        """Test mocking full executable path resolution."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.wait.return_value = None
        
        # Create a fake DART directory structure
        dart_dir = tmp_path / "DART"
        dart_dir.mkdir()
        pmo_path = dart_dir / "perfect_model_obs"
        pmo_path.write_text("#!/bin/bash\necho 'mock executable'")
        
        with patch('subprocess.Popen', return_value=mock_process) as mock_popen:
            # Simulate calling with full path
            logfile = tmp_path / "perfect_model_obs.log"
            with open(logfile, "a") as log:
                process = subprocess.Popen(
                    [str(pmo_path)],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                process.wait()
            
            # Verify correct path was used
            args, _ = mock_popen.call_args
            assert args[0] == [str(pmo_path)]
    
    def test_mock_missing_executable(self, tmp_path):
        """Test handling when perfect_model_obs executable is missing."""
        # This simulates FileNotFoundError when executable doesn't exist
        with patch('subprocess.Popen', side_effect=FileNotFoundError("perfect_model_obs not found")):
            logfile = tmp_path / "perfect_model_obs.log"
            
            with pytest.raises(FileNotFoundError, match="perfect_model_obs not found"):
                with open(logfile, "a") as log:
                    subprocess.Popen(
                        ["perfect_model_obs"],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True
                    )


class TestPerfectModelObsMultipleCalls:
    """Test mocking multiple perfect_model_obs calls in sequence."""
    
    def test_mock_multiple_sequential_calls(self, tmp_path):
        """Test mocking multiple perfect_model_obs calls."""
        # Create mock processes for multiple calls
        mock_process1 = Mock()
        mock_process1.returncode = 0
        mock_process1.wait.return_value = None
        
        mock_process2 = Mock()
        mock_process2.returncode = 0
        mock_process2.wait.return_value = None
        
        mock_process3 = Mock()
        mock_process3.returncode = 0
        mock_process3.wait.return_value = None
        
        with patch('subprocess.Popen', side_effect=[mock_process1, mock_process2, mock_process3]) as mock_popen:
            # Simulate three sequential calls
            logfile = tmp_path / "perfect_model_obs.log"
            for i in range(3):
                with open(logfile, "a") as log:
                    process = subprocess.Popen(
                        ["perfect_model_obs"],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True
                    )
                    process.wait()
                    assert process.returncode == 0
            
            # Verify all three calls were made
            assert mock_popen.call_count == 3
    
    def test_mock_mixed_success_failure(self, tmp_path):
        """Test mocking sequence with both successful and failed calls."""
        # First call succeeds, second fails, third succeeds
        mock_success1 = Mock()
        mock_success1.returncode = 0
        mock_success1.wait.return_value = None
        
        mock_failure = Mock()
        mock_failure.returncode = 1
        mock_failure.wait.return_value = None
        
        mock_success2 = Mock()
        mock_success2.returncode = 0
        mock_success2.wait.return_value = None
        
        with patch('subprocess.Popen', side_effect=[mock_success1, mock_failure, mock_success2]):
            logfile = tmp_path / "perfect_model_obs.log"
            
            # First call succeeds
            with open(logfile, "a") as log:
                process1 = subprocess.Popen(["perfect_model_obs"], stdout=log,
                                           stderr=subprocess.STDOUT, text=True)
                process1.wait()
            assert process1.returncode == 0
            
            # Second call fails
            with open(logfile, "a") as log:
                process2 = subprocess.Popen(["perfect_model_obs"], stdout=log,
                                           stderr=subprocess.STDOUT, text=True)
                process2.wait()
            assert process2.returncode == 1
            
            # Third call succeeds
            with open(logfile, "a") as log:
                process3 = subprocess.Popen(["perfect_model_obs"], stdout=log,
                                           stderr=subprocess.STDOUT, text=True)
                process3.wait()
            assert process3.returncode == 0
